from pandas import read_excel, DataFrame, to_datetime, isnull, Timestamp
import sys
from codecs import open
from datetime import datetime, timedelta
from pycountry import countries
import geojson
from drivesyncer.drivesyncer import DriveSyncer
from reporter.reporter import Reporter
from musicbrainz.musicbrainzartistsbelgium import MusicBrainzArtistsBelgium
from leechers.songkickleecher import SongkickLeecher
from leechers.bandsintownleecher import BandsInTownLeecher
from leechers.setlistleecher import SetlistFmLeecher
from leechers.facebookscraper import FacebookScraper
from leechers.datakunstenbe import DataKunstenBeConnector
from havelovewilltravel.havelovewilltravel import HaveLoveWillTravel


class Grabber(object):
    def __init__(self, update_from_musicbrainz=True):
        self.syncdrive = DriveSyncer()
        self.now = datetime.now()
        self.reporter = Reporter(self.now, self.syncdrive)
        self.mbab = MusicBrainzArtistsBelgium(update=update_from_musicbrainz)
        self.songkickleecher = SongkickLeecher()
        self.bandsintownleecher = BandsInTownLeecher()
        self.setlistleecher = SetlistFmLeecher()
        self.facebookleecher = FacebookScraper()
        self.hlwt = HaveLoveWillTravel()
        self.previous = None
        self.previous_bare = None
        self.current = DataFrame()
        self.df = None
        self.diff_event_ids = None
        with open("resources/belgium/admin_level_2.geojson", "r", "utf-8") as f:
            self.belgium = geojson.load(f)

    def grab(self):
        print("making snapshot")
        self.reporter.take_snapshot_of_status("old")

        print("syncing from drive")
        self.syncdrive.set_resource_files_ids()
        self.syncdrive.downstream()

        print("loading mb")
        self.mbab.calculate_concerts_abroad()
        self.mbab.load_list()
        self.mbab.make_genre_mapping()
        if self.mbab.update:
            self.mbab.update_list()

        print("starting the leech process")
        self.leech([self.songkickleecher, self.bandsintownleecher, self.setlistleecher, self.facebookleecher])
        print("adding manual concerts")
        self.add_manual_concerts() #TODO lukt dat met remarks? rhythm junks
        #print("adding podiumfestivalinfo concerts")
        #self.add_podiumfestivalinfo_concerts()

#        print("adding data.kunsten.be concerts")
#        self.add_datakunstenbe_concerts()

        self.combine_versions()
        self.clean_country_names()
        self._clean_names("stad", "stad_clean", "resources/city_cleaning.xlsx")
        self._clean_names("venue", "venue_clean", "resources/venue_cleaning.xlsx")
        self.handle_ambiguous_artists()
        self.make_concerts()
        self.infer_cancellations()
        self._set_source_outlinks_per_concert()
        self.identify_new_and_updated_concerts()
        self.hlwt.set_data(self.df, self.diff_event_ids)  # zet data
        self.hlwt.send_data_to_mr_henry()
        self.persist_output()
        self.reporter.take_snapshot_of_status("current")
        self.syncdrive.upstream()
#        self.reporter.do()

    def leech(self, leechers):
        for leecher in leechers:
            print("commencing leech for", leecher.platform)
            leecher.set_platform_identifiers()
            leecher.set_events_for_identifiers()
        lines = sum([leecher.events for leecher in leechers], [])
        self.current = DataFrame(lines)

    def add_manual_concerts(self):
        manual = read_excel("resources/manual.xlsx")
        manual["datum"] = [Timestamp(datum.date()) for datum in to_datetime(manual["datum"].values)]
        manual["source"] = ["manual"] * len(manual.index)
        self.current = self.current.append(manual, ignore_index=True, sort=True)

    def add_podiumfestivalinfo_concerts(self):
        pci = read_excel("resources/podiumfestivalinfo.xlsx")
        pci["datum"] = [Timestamp(datum.date()) for datum in to_datetime(pci["datum"].values)]
        pci["einddatum"] = [Timestamp(datum.date()) if not isnull(datum) else None for datum in to_datetime(pci["einddatum"].values)]
        pci["artiest_mb_id"] = [self._match_artist_name_to_mbid(artiest_mb_naam) for artiest_mb_naam in pci["artiest_mb_naam"].values]
        self.current = self.current.append(pci, ignore_index=True, sort=True)

    def _match_artist_name_to_mbid(self, artiest_mb_naam):
        matches = self.mbab.lijst[self.mbab.lijst["band"] == artiest_mb_naam]["mbid"].values
        return matches[0] if len(matches) > 0 else None

    def add_datakunstenbe_concerts(self):
        dkbc = DataKunstenBeConnector(root="./leechers/")
        dkbc.get_concerts_abroad()
        self.current = self.current.append(dkbc.concerts, ignore_index=True, sort=True)

    @staticmethod
    def _fix_weird_symbols(x):
        return ''.join([str(c) for c in str(x) if ord(str(c)) > 31 or ord(str(c)) == 9])

    def _fix_weird_symbols_in_columns(self, columns):
        for column in columns:
            self.current[column] = self.current[column].map(lambda x: self._fix_weird_symbols(x))

    def combine_versions(self):
        print("update previous version")
        print("\tfixing weird symbols")
        self._fix_weird_symbols_in_columns(["titel", "artiest", "venue", "artiest", "stad", "land"])

        print("\tadding a last seen on column")
        self.current["last_seen_on"] = [self.now.date()] * len(self.current.index)

        print("\treading in the previous concerts")
        self.previous = read_excel("output/latest.xlsx")
        self.previous_bare = self.previous.copy()

        print("\tdropping generated columns")
        for column in ["concert_id", "stad_clean", "land_clean", "iso_code_clean", "venue_clean", "visible", "source_0", "source_link_0", "source_1", "source_link_1", "source_2", "source_link_2", "source_3", "source_link_3", "source_4", "source_link_4"]:
            try:
                self.previous_bare.drop(column, 1, inplace=True)
            except ValueError:
                continue

        print("combing the two datasets")
        self.df = self.previous_bare.append(self.current, ignore_index=True, sort=True)

        #print("\tfixing the last seen on date")
        #self.df["last_seen_on"] = [lso.date() if isinstance(lso, datetime) else lso for lso in self.df["last_seen_on"]]

        print("\tapplying current updates to previous concerts")
        # make sure that an update in title, date and enddate is reflected in the first entry
        for column in ["titel", "datum", "einddatum"]:
            print("\t\tdoing the {0}".format(column))
            self._update_field_based_on_new_leech(column)

        print("\tdropping duplicates")
        self.df.drop_duplicates(subset=["artiest_mb_id", "event_id"], keep="first", inplace=True)  # keep first to reflect any updates

        print("\tadding the genres")
        self.df["maingenre"] = [self.mbab.genres[mbid] if mbid in self.mbab.genres else None for mbid in self.df["artiest_mb_id"]]

        print("\tapplying last seen on date to today")
        self._update_last_seen_on_dates_of_previous_events_that_are_still_current()

    def identify_new_and_updated_concerts(self):
        with open("resources/kolomvolgorde.txt", "r") as f:
            kolomvolgorde = [kolom.strip() for kolom in f.read().split("\n")]
        kolomvolgorde.remove("last_seen_on")
        kolomvolgorde.remove("concert_id")
        merged = self.previous.merge(self.df, how="outer", indicator=True, on="event_id")
        new_and_updated = merged[merged["_merge"] == "right_only"]
        self.diff_event_ids = new_and_updated["event_id"]

    def _update_field_based_on_new_leech(self, field):
        tmp = self.df[["event_id", field]].drop_duplicates()
        cntr = tmp["event_id"].value_counts()
        updates = {}
        for key in cntr[cntr > 1].index:
            key_ = self.df[self.df["event_id"] == key]
            updates[key] = (key_.index[0], key_[field].unique())
        for update in updates:
            idx = updates[update][0]
            update_values = updates[update][1]
            if len(update_values) > 1:
                self.df.at[idx, field] = update_values[-1]  # overwrite with latest value

    def _update_last_seen_on_dates_of_previous_events_that_are_still_current(self):
        prev_also_in_cur = self.previous[["event_id", "artiest_mb_id"]].isin(self.current[["event_id", "artiest_mb_id"]].to_dict(orient="list"))
        prev_indices = (prev_also_in_cur["event_id"] & prev_also_in_cur["artiest_mb_id"])
        events_artiesten = (self.previous["event_id"][prev_indices].values, self.previous["artiest_mb_id"][prev_indices].values)
        event_ids = []
        artiest_merge_names = []
        for i in range(0, len(events_artiesten[0])):
            event_ids.append(events_artiesten[0][i])
            artiest_merge_names.append(events_artiesten[1][i])
        idxs = self.df[(self.df["event_id"].isin(event_ids)) & (self.df["artiest_mb_id"].isin(artiest_merge_names))].index
        self.df.at[idxs, "last_seen_on"] = self.now.date()

    @staticmethod
    def _convert_cleaned_country_name_to_full_name(twolettercode):
        try:
            return countries.get(alpha_2=twolettercode).name
        except (KeyError, AttributeError):
            return None

    def clean_country_names(self):
        clean_countries = []
        clean_iso_codes = []
        country_cleaning = read_excel("resources/country_cleaning.xlsx")
        country_cleaning_additions = set()
        for land in self.df["land"]:
            land = str(land).strip()
            if len(land) == 2:
                if land == "UK":
                    land = "GB"
                clean_countries.append(self._convert_cleaned_country_name_to_full_name(land.upper()))
                clean_iso_codes.append(land.upper())
            else:
                try:
                    clean_country = countries.get(name=land).alpha_2
                except (KeyError, AttributeError) as e:
                    if land in country_cleaning["original"].values:
                        clean_country = country_cleaning[country_cleaning["original"] == land]["clean"].iloc[0]
                    else:
                        country_cleaning_additions.add(land)
                        clean_country = None
                clean_countries.append(self._convert_cleaned_country_name_to_full_name(clean_country))
                clean_iso_codes.append(clean_country)
        country_cleaning.append(DataFrame([{"original": land, "clean": None} for land in country_cleaning_additions]), ignore_index=True).drop_duplicates().to_excel("resources/country_cleaning.xlsx")
        self.df["land_clean"] = clean_countries
        self.df["iso_code_clean"] = clean_iso_codes

    def _clean_names(self, column, column_clean, resource):
        item_cleaning_additions = set()
        clean_items = []
        item_cleaning = read_excel(resource)
        for item in self.df[column]:
            if item in item_cleaning["original"].values:
                clean_item = item_cleaning[item_cleaning["original"] == item]["clean"].iloc[0]
            else:
                item_cleaning_additions.add(item)
                clean_item = None
            clean_items.append(clean_item)
        item_cleaning.append(DataFrame([{"original": item, "clean": None} for item in item_cleaning_additions]), ignore_index=True).drop_duplicates().to_excel(resource)
        self.df[column_clean] = clean_items

    def handle_ambiguous_artists(self):
        merge_artists_xlsx = read_excel("resources/merge_artists.xlsx")
        merge_artists = {row[1]["original"]: row[1]["clean"] for row in merge_artists_xlsx.iterrows()}
        for idx in self.df[isnull(self.df["artiest_merge_naam"])].index:
            naam = self.df.loc[idx, "artiest_mb_naam"]
            self.df.at[idx, "artiest_merge_naam"] = merge_artists[naam] if naam in merge_artists else naam

    def _make_gig_triples(self):
        return set([tuple(x) for x in self.df[["artiest_merge_naam", "datum", "stad_clean"]].values])

    def _assign_concert_ids(self, artist_date_city_triples):
        gig_triple_id = 0
        for gig_triple in artist_date_city_triples:
            if gig_triple_id % 25000 == 0:
                print("\t\t", gig_triple_id, "of", len(artist_date_city_triples))
            events = self.df[(self.df["artiest_merge_naam"] == gig_triple[0]) &
                             (self.df["datum"] == gig_triple[1]) &
                             (self.df["stad_clean"] == gig_triple[2])]
            for i in events.index:
                self.df.at[i, "concert_id"] = gig_triple_id
            gig_triple_id += 1

    @staticmethod
    def __inside_polygon(x, y, points):
        n = len(points)
        inside = False
        p1x, p1y = points[0]
        for i in range(1, n + 1):
            p2x, p2y = points[i % n]
            if y > min(p1y, p2y):
                if y <= max(p1y, p2y):
                    if x <= max(p1x, p2x):
                        if p1y != p2y:
                            xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                        if p1x == p2x or x <= xinters:
                            inside = not inside
            p1x, p1y = p2x, p2y
        return inside

    def _concert_is_in_belgium(self, concert):
        return True in [self.__inside_polygon(float(concert["longitude"]), float(concert["latitude"]), list(geojson.utils.coords(self.belgium[i]))) for i in range(0, len(self.belgium["features"]))] if concert["longitude"] is not None else False

    def _select_visibility_per_concert(self):
        self.df["visible"] = [False] * len(self.df.index)
        for concert_id in self.df["concert_id"].unique():
            concerts = self.df[self.df["concert_id"] == concert_id]
            in_belgium = True in [self._concert_is_in_belgium(concert) for concert in concerts.to_dict("records")]
            if not in_belgium:
                concert_cancellations = concerts.apply(self._is_cancellation, axis=1)
                concert_source_values = concerts["source"].values
                psv = ["manual", "datakunstenbe", "podiumfestivalinfo", "songkick", "bandsintown", "setlist", "facebook"]
                source = self._establish_optimal_source(concert_source_values, concert_cancellations, psv)
                if source:
                    self.df.at[concerts[concerts["source"] == source].index[0], "visible"] = True
        self.df["ignore"].fillna(False, inplace=True)  # fill rest of ignored concerts with False

    def _set_source_outlinks_per_concert(self):
        for i in range(0, 10, 1):
            self.df["source_" + str(i)] = [None] * len(self.df.index)
            self.df["source_link_" + str(i)] = [None] * len(self.df.index)
        for concert_id in self.df["concert_id"].unique():
            concerts = self.df[self.df["concert_id"] == concert_id]
            event_ids = concerts["event_id"].values
            visible_event_ids = concerts[concerts["visible"]]["event_id"].values
            if len(visible_event_ids) > 0:
                visible_event_id = visible_event_ids[0]
                for i, event_id in enumerate(event_ids):
                    source, source_link = self._establish_source_hyperlink(event_id)
                    event_id__index_ = concerts[concerts["event_id"] == visible_event_id].index[0]
                    self.df.at[event_id__index_, "source_" + str(i)] = source
                    self.df.at[event_id__index_, "source_link_" + str(i)] = source_link

    @staticmethod
    def _establish_source_hyperlink(event_id):
        if "facebook" in event_id:
            return "Facebook", "https://www.facebook.com/events/" + event_id.split("facebook")[-1]
        elif "songkick" in event_id:
            return "Songkick", "https://www.songkick.com/concerts/" + event_id.split("_")[-1]
        elif "bandsintown" in event_id:
            return "BandsInTown", "http://bandsintown.com/e/" + event_id.split("_")[-1]
        elif "setlist" in event_id:
            return "Setlist.fm", "https://www.setlist.fm/setlist/a/0/b-" + event_id.split("setlist")[-1]
        elif "podiuminfo" in event_id:
            return "Podium/Festivalinfo", "http://festivalinfo.nl"
        elif "datakunstenbe" in event_id:
            return "Kunstenpunt", "http://data.kunsten.be"
        else:
            return None, None

    @staticmethod
    def _establish_optimal_source(concert_source_values, concert_cancellations, potential_source_values):
        source = None
        concert_source_value_established = False
        for potential_source_value in potential_source_values:
            if not concert_source_value_established:
                source = potential_source_value if potential_source_value in concert_source_values else None
                if source:
                    cancelled = concert_cancellations.iloc[concert_source_values.tolist().index(source)]
                    if not cancelled:
                        concert_source_value_established = True
                    else:
                        source = None
        return source

    def make_concerts(self):
        print("identifying the concerts")
        artist_date_city_triples = self._make_gig_triples()

        print("\tassigning concert ids")
        self._assign_concert_ids(artist_date_city_triples)

        print("\tsetting visibility")
        self._select_visibility_per_concert()

        print("\tresolving festival date ranges")
        self._set_precise_date_for_festivals()

        self.df.loc[(self.df["source"] == "facebook") & (self.df["stad"] == "None"), "visible"] = False  # concerts from facebook without decent city information should be ignored

    def _set_precise_date_for_festivals(self):
        visible_festivals = self.df[(self.df["event_type"].str.lower() == "festival") &
                                    (self.df["source"].isin(["songkick", "podiumfestivalinfo", "facebook"])) &
                                    (self.df["visible"])]
        for row in visible_festivals.iterrows():
            festival_index = row[0]
            begindatum = row[1]["datum"] if not isnull(row[1]["datum"]) else Timestamp(datetime(1970, 1, 1))
            einddatum = row[1]["einddatum"] if not isnull(row[1]["einddatum"]) else begindatum
            artiest_mb_id = row[1]["artiest_mb_id"]
            stad_clean = row[1]["stad_clean"]
            precise_events = self.df[
                (self.df["source"].isin(["bandsintown", "facebook", "datakunstenbe", "manual", "setlist", "songkick"])) &
                (self.df["datum"].between(begindatum, einddatum)) &
                (self.df["event_type"] != "festival") &
                (self.df["artiest_mb_id"] == artiest_mb_id) &
                (self.df["stad_clean"] == stad_clean)]
            precise_events_source_values = precise_events["source"].values
            concert_cancellations = precise_events.apply(self._is_cancellation, axis=1)
            potential_source_values = ["manual", "datakunstenbe", "bandsintown", "setlist", "facebook", "songkick"]
            source = self._establish_optimal_source(precise_events_source_values, concert_cancellations, potential_source_values)
            if source:
                precise_concert_index = precise_events[precise_events["source"] == source].index[0]
                self.df.at[precise_concert_index, "visible"] = True
                precise_concert_id = self.df.loc[precise_concert_index]["concert_id"]
                self.df.at[festival_index, "concert_id"] = precise_concert_id
                self.df.at[festival_index, "visible"] = False

    def _is_cancellation(self, row):
        return (Timestamp(row["datum"]) > Timestamp(self.now.date())) & (Timestamp(row["last_seen_on"]) < Timestamp(self.now.date() - timedelta(days=2)))

    def infer_cancellations(self):
        print("inferring cancellations")
        self.df["cancelled"] = self.df.apply(self._is_cancellation, axis=1)

    def persist_output(self):
        print("writing to file")
        with open("resources/kolomvolgorde.txt", "r") as f:
            kolomvolgorde = [kolom.strip() for kolom in f.read().split("\n")]
        self.df.to_excel("output/latest.xlsx", columns=kolomvolgorde)


if __name__ == "__main__":
    update_from_musicbrainz = (sys.argv[1] == "True") if len(sys.argv) > 1 else False
    grabber = Grabber(update_from_musicbrainz)
    grabber.grab()
