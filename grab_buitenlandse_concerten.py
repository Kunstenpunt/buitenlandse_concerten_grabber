from pandas import read_excel, DataFrame, to_datetime
from re import sub
from musicbrainzngs import set_useragent, search_artists, get_area_by_id, musicbrainz, get_artist_by_id
from codecs import open
from time import sleep
from json import loads
from requests import get, exceptions
from math import ceil
from datetime import datetime
import bandsintown
from urllib import parse as urlparse
import facebook
from dateparser import parse as dateparse
import psycopg2
from configparser import ConfigParser
from pycountry import countries
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


class PlatformLeecher(object):
    """Abstract class defining the interface for every class that will leech concerts from an online platform"""

    def __init__(self):
        self.platform_identifiers = []
        self.platform_access_granter = None
        self.events = []
        self.platform = None

    def set_platform_identifiers(self):
        lst = read_excel("resources/belgian_mscbrnz_artists.xlsx")
        ignore_list = read_excel("resources/ignore_list.xlsx")
        bands_done = set()
        for i in lst.index:
            row = lst.ix[i]
            if row["mbid"] not in ignore_list["mbid"].values:
                if row[self.platform] is not None and row[self.platform] != "None" and row["band"] not in bands_done:
                    self.platform_identifiers.append((row[["band", "mbid", self.platform]]))
                else:
                    bands_done.add(row["band"])
            else:
                print("ignoring", row["band"])

    def set_events_for_identifiers(self):
        for band, mbid, url in self.platform_identifiers:
            print(self.platform, band)
            self.set_events_for_identifier(band, mbid, url)

    def set_events_for_identifier(self, band, mbid, url):
        raise NotImplementedError

    def map_platform_to_schema(self, event, band, mbid, other):
        raise NotImplementedError


class SongkickLeecher(PlatformLeecher):
    def __init__(self):
        super().__init__()
        with open("resources/songkick_api_key.txt") as f:
            self.platform_access_granter = f.read()
        self.platform = "songkick"
        self.past_events_url = "http://api.songkick.com/api/3.0/artists/{0}/gigography.json?apikey={1}&page={2}"
        self.future_events_url = "http://api.songkick.com/api/3.0/artists/{0}/calendar.json?apikey={1}&page={2}"

    def set_events_for_identifier(self, band, mbid, url):
        artist_id, artist_name = url.split("/")[-1].split("-")[0], " ".join(url.split("/")[-1].split("-")[1:])
        self.get_events(self.past_events_url, artist_id, artist_name, band, mbid)
        self.get_events(self.future_events_url, artist_id, artist_name, band, mbid)

    def get_events(self, base_url, artistid, artistname, band, mbid):
        page = 1
        url = base_url.format(artistid, self.platform_access_granter, page)
        json_response = loads(get(url).text)
        resultspaga = json_response["resultsPage"]
        amount_events = resultspaga["totalEntries"] if "totalEntries" in resultspaga else 0
        amount_pages = ceil(amount_events / 50.0)
        while page <= amount_pages:
            if json_response["resultsPage"]["status"] == "ok":
                for event in json_response["resultsPage"]["results"]["event"]:
                    self.events.append(self.map_platform_to_schema(event, band, mbid, {"artist_id": artistid,
                                                                                       "artist_name": artistname}))
                page += 1
                url = base_url.format(artistid, self.platform_access_granter, page)
                json_response = loads(get(url).text)

    def map_platform_to_schema(self, event, band, mbid, other):
        return {
            "titel": event["displayName"].strip(),
            "datum": dateparse(event["start"]["date"]).date(),
            "eindatum": dateparse(event["end"]["date"]).date(),
            "artiest": other["artist_name"],
            "artiest_id": "songkick_" + str(other["artist_id"]),
            "artiest_mb_naam": band,
            "artiest_mb_id": mbid,
            "stad": event["location"]["city"].split(",")[0].strip(),
            "land": event["location"]["city"].split(",")[-1].strip(),
            "venue": event["displayName"].strip() if event["type"] == "Festival" else event["venue"]["displayName"].strip(),
            "latitude": event["venue"]["lat"],
            "longitude": event["venue"]["lng"],
            "source": self.platform,
            "event_id": "songkick_" + str(event["id"]),
            "event_type": event["type"]
        }


class BandsInTownLeecher(PlatformLeecher):
    def __init__(self):
        super().__init__()
        self.bitc = bandsintown.Client("kunstenpunt")
        self.platform = "bandsintown"

    def set_events_for_identifier(self, band, mbid, url):
        period = "1900-01-01,2050-01-01"

        bandnaam = urlparse.unquote(url.split("/")[-1].split("?came_from")[0])
        events = self.bitc.events(bandnaam, date=period)
        while "errors" in events:
            if "Rate limit exceeded" in events["errors"]:
                print("one moment!")
                sleep(60.0)
                events = self.bitc.events(bandnaam, date=period)
            else:
                events = []

        for concert in events:
            self.events.append(self.map_platform_to_schema(concert, band, mbid, {}))

    def map_platform_to_schema(self, concert, band, mbid, other):
        return {
            "datum": dateparse(concert["datetime"]).date(),
            "land": (concert["venue"]["country"]).strip(),
            "stad": (concert["venue"]["city"]).strip(),
            "venue": (concert["venue"]["place"]).strip(),
            "titel": (concert["title"]).strip(),
            "artiest": self.__get_artist_naam(concert),
            "artiest_mb_naam": band,
            "artiest_id": "bandsintown_" + str(concert["artist_id"]),
            "artiest_mb_id": mbid,
            "event_id": "bandsintown_" + str(concert["id"]),
            "latitude": (concert["venue"]["latitude"]),
            "longitude": (concert["venue"]["longitude"]),
            "source": self.platform
        }

    @staticmethod
    def __get_artist_naam(concert):
        for artist in concert["artists"]:
            if artist["id"] == concert["artist_id"]:
                return artist["name"]


class FacebookEventLeecher(PlatformLeecher):
    def __init__(self):
        super().__init__()
        with open("resources/facebook_access_token.txt") as f:
            fb_token = f.read()
        self.graph = facebook.GraphAPI(access_token=fb_token, version=2.10)
        self.platform = "facebook"

    def set_events_for_identifier(self, band, mbid, url):
        page_label = url.split("/")[-1]
        if "-" in page_label:
            page_label = page_label.split("-")[-1]
        try:
            events = self.graph.get_connections(id=page_label, connection_name="events")
            if "data" in events:
                for concert in events["data"]:
                    self.events.append(self.map_platform_to_schema(concert, band, mbid, {"page_label": page_label}))
        except facebook.GraphAPIError as e:
            pass
        except exceptions.ConnectionError as e:
            self.set_events_for_identifier(band, mbid, url)

    def map_platform_to_schema(self, concert, band, mbid, other):
        return {
            "titel": concert["name"] if "name" in concert else None,
            "datum": dateparse(concert["start_time"]).date(),
            "artiest": band,
            "artiest_id": "facebook_" + other["page_label"],
            "artiest_mb_naam": band,
            "artiest_mb_id": mbid,
            "stad": concert["place"]["location"]["city"] if "place" in concert and "location" in concert["place"] and "city" in concert["place"]["location"] else None,
            "land": concert["place"]["location"]["country"] if "place" in concert and "location" in concert["place"] and "country" in concert["place"]["location"] else None,
            "venue": concert["place"]["name"] if "place" in concert else None,
            "latitude": concert["place"]["location"]["latitude"] if "place" in concert and "location" in concert["place"] and "latitude" in concert["place"]["location"] else None,
            "longitude": concert["place"]["location"]["longitude"] if "place" in concert and "location" in concert["place"] and "longitude" in concert["place"]["location"] else None,
            "source": self.platform,
            "event_id": "facebook" + concert["id"]
        }


class SetlistFmLeecher(PlatformLeecher):
    def __init__(self):
        super().__init__()
        with open("resources/setlist_api_key.txt", "r") as f:
            self.platform_access_granter = f.read()
        self.platform = "setlist"

    def set_events_for_identifier(self, band, mbid, url):
        total_hits = 1
        p = 1
        retrieved_hits = 0
        while retrieved_hits < total_hits:
            headers = {"x-api-key": self.platform_access_granter, "Accept": "application/json"}
            r = get("https://api.setlist.fm/rest/1.0/artist/{1}/setlists?p={0}".format(p, mbid), headers=headers)
            response = loads(r.text)
            if "setlist" in response:
                for concert in response["setlist"]:
                    self.events.append(self.map_platform_to_schema(concert, band, mbid, {}))
                total_hits = int(response["total"])
                retrieved_hits += int(response["itemsPerPage"])
            else:
                total_hits = 0
            p += 1

    def map_platform_to_schema(self, concert, band, mbid, other):
        return {
            "titel": concert["info"] if "info" in concert else None,
            "datum": dateparse(concert["eventDate"], ["%d-%m-%Y"]).date(),
            "artiest": concert["artist"]["name"],
            "artiest_id": "setlist_" + concert["artist"]["url"],
            "artiest_mb_naam": band,
            "artiest_mb_id": mbid,
            "stad": concert["venue"]["city"]["name"],
            "land": concert["venue"]["city"]["country"]["code"],
            "venue": concert["venue"]["name"],
            "latitude": concert["venue"]["city"]["coords"]["lat"] if "lat" in concert["venue"]["city"]["coords"] else None,
            "longitude": concert["venue"]["city"]["coords"]["long"] if "long" in concert["venue"]["city"]["coords"] else None,
            "source": "setlist",
            "event_id": "setlist" + concert["id"]
        }


class MusicBrainzArtistsBelgium(object):
    def __init__(self, update=False):
        concerts = read_excel("output/latest.xlsx")
        concerts_abroad_future = concerts[(concerts["land_clean"] != "BE") & (concerts["datum"] > datetime.now())]
        self.aantal_concerten_per_mbid = concerts_abroad_future.groupby(["artiest_mb_id"])["event_id"].count()
        set_useragent("kunstenpunt", "0.1", "github.com/kunstenpunt")
        self.lijst = None
        self.maingenres = {}
        if update:
            self.make_genre_mapping()
            self.update_list()

    def make_genre_mapping(self):
        self.load_list()
        for row in self.lijst.iterrows():
            key = row[1]["mbid"]
            value = row[1]["maingenre"]
            self.maingenres[key] = value

    def load_list(self):
        self.lijst = read_excel("resources/belgian_mscbrnz_artists.xlsx")

    @staticmethod
    def __get_land(artist):
        if "area" in artist:
            area = artist["area"]
            recurse = True
            while recurse:
                recurse = False
                areas = get_area_by_id(area["id"], includes="area-rels")["area"]["area-relation-list"]
                for test_area in areas:
                    if "direction" in test_area and test_area["direction"] == "backward":
                        area = test_area["area"]
                        recurse = True
            return area
        else:
            return {}

    @staticmethod
    def __get_rel_url(artist, urltype, domain=None):
        if "url-relation-list" in artist["artist"]:
            for url in artist["artist"]["url-relation-list"]:
                if url["type"] == urltype:
                    if domain:
                        if domain in url["target"]:
                            return url["target"]
                    else:
                        return url["target"]

    @staticmethod
    def __make_artist_name(n):
        return sub(r"[^\w\s\d]", "", sub(r"[\[\(].+?[\)\]]", "", n)).strip()

    @staticmethod
    def __get_parts_of(area_id):
        part_of_ids = []
        areas = None
        while areas is None:
            try:
                sleep(1.0)
                areas = get_area_by_id(area_id, includes="area-rels")["area"]["area-relation-list"]
            except musicbrainz.NetworkError:
                sleep(25.0)
        for area in areas:
            if area["type"] == "part of" and "direction" not in area:
                part_of_ids.append((area["area"]["id"], area["area"]["name"]))
        return part_of_ids

    @staticmethod
    def __search_artists_in_area(area, limit, offset):
        artists = {"artist-list": [], "artist-count": -1}
        while artists["artist-count"] < 0:
            try:
                sleep(1.0)
                artists_area = search_artists(area=area, limit=limit, offset=offset)
                sleep(1.0)
                artists_beginarea = search_artists(beginarea=area, limit=limit, offset=offset)
                artists['artist-list'] = artists_area["artist-list"] + artists_beginarea["artist-list"]
                artists['artist-count'] = artists_area["artist-count"] + artists_beginarea["artist-count"]
            except musicbrainz.NetworkError:
                sleep(25.0)
        return artists

    def __number_of_concerts(self, mbid):
        try:
            return self.aantal_concerten_per_mbid.loc[mbid]
        except KeyError:
            return 0

    def __is_on_ignore_list(self, mbid):
        ignore_list = read_excel("resources/ignore_list.xlsx")
        return mbid in ignore_list["mbid"]

    def update_list(self):
        area_ids = [("5b8a5ee5-0bb3-34cf-9a75-c27c44e341fc", "Belgium")]
        new_parts = self.__get_parts_of(area_ids[0][0])
        area_ids.extend(new_parts)
        while len(new_parts) > 0:
            new_new_parts = []
            for new_part in new_parts:
                print("nieuwe locatie", new_part[1])
                parts = self.__get_parts_of(new_part[0])
                new_new_parts.extend(parts)
                area_ids.extend(parts)
            new_parts = new_new_parts
        belgium = []
        for area_id in area_ids:
            print("finding artists in", area_id)
            offset = 0
            limit = 100
            area_hits = []
            total_search_results = 1
            while offset < total_search_results:
                search_results = self.__search_artists_in_area(area_id[1], limit, offset)
                for hit in list(search_results["artist-list"]):
                    if ("area" in hit and hit["area"]["id"] == area_id[0]) or ("begin-area" in hit and hit["begin-area"]["id"] == area_id[0]):
                        artist = None
                        while artist is None:
                            try:
                                sleep(1.0)
                                artist = get_artist_by_id(hit["id"], includes=["url-rels"])
                            except musicbrainz.NetworkError:
                                sleep(25.0)
                        songkick_url = self.__get_rel_url(artist, "songkick")
                        bandsintown_url = self.__get_rel_url(artist, "bandsintown")
                        setlistfm_url = self.__get_rel_url(artist, "setlistfm")
                        facebook_url = self.__get_rel_url(artist, "social network", "facebook.com")
                        print(hit["name"])
                        lijn = {
                            "band": hit["name"],
                            "mbid": hit["id"],
                            "area": hit["area"]["name"] if "area" in hit else None,
                            "begin-area": hit["begin-area"]["name"] if "begin-area" in hit else None,
                            "begin": hit["life-span"]['begin'] if "life-span" in hit and "begin" in hit["life-span"] else None,
                            "end": hit["life-span"]["end"] if "life-span" in hit and "end" in hit["life-span"] else None,
                            "ended": hit["life-span"]["ended"] if "life-span" in hit and "ended" in hit["life-span"] else None,
                            "disambiguation": hit["disambiguation"] if "disambiguation" in hit else None,
                            "facebook": str(facebook_url),
                            "songkick": str(songkick_url),
                            "bandsintown": str(bandsintown_url),
                            "setlist": str(setlistfm_url),
                            "number_of_concerts": self.__number_of_concerts(hit["id"]),
                            "on_ignore_list": self.__is_on_ignore_list(hit["id"]),
                            "maingenre": self.maingenres[hit["id"]] if hit["id"] in self.maingenres else "Rest"
                        }
                        area_hits.append(lijn)
                offset += limit
                total_search_results = search_results["artist-count"]
            belgium.extend(area_hits)
        DataFrame(belgium).drop_duplicates().to_excel("resources/belgian_mscbrnz_artists.xlsx")


class DataKunstenBeConnector(object):
    def __init__(self):
        cfg = ConfigParser()
        cfg.read("resources/db.cfg")
        knst = psycopg2.connect(host=cfg['db']['host'], port=cfg['db']['port'],
                                database=cfg['db']['db'], user=cfg['db']['user'],
                                password=cfg['db']['pwd'])
        knst.set_client_encoding('UTF-8')
        self.cur = knst.cursor()
        self.concerts = None

    def get_concerts_abroad(self):
        sql = """
            SELECT
              s.id, d.year, d.month, d.day,
              p.full_name artiest_volledige_naam,
              o.name organisatie_naam, o.city organisatie_stad, o_countries.iso_code organisatie_land,
              l_organisations.city_nl organisatie_locatie_stad, l_organisations_countries.iso_code organisatie_locatie_land,
              l_a_organisations.city_nl organisatie_adres_locatie_stad, l_a_organisations_countries.iso_code organisatie_adres_locatie_land,
              v.name venue_naam, l_venues.city_nl venue_locatie_stad, l_venues_countries.iso_code venue_locatie_land

            FROM production.shows s

            JOIN production.show_types st
            ON s.show_type_id = st.id

            JOIN production.date_isaars d
            ON s.date_id = d.id

            JOIN production.relationships rel_show_person
            ON s.id = rel_show_person.show_id
            JOIN production.people p
            ON rel_show_person.person_id = p.id

            LEFT JOIN production.countries p_countries
            ON p.country_id = p_countries.id

            LEFT JOIN production.locations p_locations
            ON p.location_id = p_locations.id
            LEFT JOIN production.countries p_locations_countries
            ON p_locations.country_id = p_locations_countries.id

            LEFT JOIN production.locations p_c_locations
            ON p.current_location_id = p_c_locations.id
            LEFT JOIN production.countries p_c_locations_countries
            ON p_c_locations.country_id = p_c_locations_countries.id

            JOIN production.organisations o
            ON s.organisation_id = o.id
            LEFT JOIN production.countries o_countries
            ON o.country_id = o_countries.id
            LEFT JOIN production.locations l_organisations
            ON o.location_id = l_organisations.id
            LEFT JOIN production.countries l_organisations_countries
            ON l_organisations.country_id = l_organisations_countries.id

            LEFT JOIN production.locations l_a_organisations
            ON o.address_location_id = l_a_organisations.id
            LEFT JOIN production.countries l_a_organisations_countries
            ON l_a_organisations.country_id = l_a_organisations_countries.id

            JOIN production.relationships rel_show_genre
            ON s.id = rel_show_genre.show_id
            JOIN production.genres g
            ON rel_show_genre.genre_id = g.id

            LEFT JOIN production.venues v
            ON s.venue_id = v.id

            LEFT JOIN production.countries v_countries
            ON v.country_id = v_countries.id
            LEFT JOIN production.locations l_venues
            ON v.location_id = l_venues.id
            LEFT JOIN production.countries l_venues_countries
            ON l_venues.country_id = l_venues_countries.id

            WHERE st.id IN (456, 457)

            ORDER BY
              d.year DESC, d.month DESC, d.day DESC
        """
        self.cur.execute(sql)
        os = self.cur.fetchall()
        df = DataFrame(os, columns=["id", "year", "month", "day", "artiest", "organisatie_naam", "organisatie_stad1", "organisatie_land1", "organisatie_stad2", "organisatie_land2", "organisatie_stad3", "organisatie_land3", "venue_naam", "venue_stad", "venue_land"])
        df["event_id"] = ["datakunstenbe_" + str(row[1]["id"]) for row in df.iterrows()]
        df["datum"] = [datetime(int(row[1]["year"]), int(row[1]["month"]), int(row[1]["day"]) if row[1]["day"] > 0 else 1).date() for row in df.iterrows()]
        df["stad"] = [row[1]["venue_stad"] if row[1]["venue_stad"] is not None else row[1]["organisatie_stad3"] if row[1]["organisatie_stad3"]  is not None else row[1]["organisatie_stad2"] if row[1]["organisatie_stad2"]  is not None else row[1]["organisatie_stad1"] for row in df.iterrows()]
        df["land"] = [row[1]["venue_land"] if row[1]["venue_land"] else row[1]["organisatie_land3"] if row[1]["organisatie_land3"] else row[1]["organisatie_land2"] if row[1]["organisatie_land2"] else row[1]["organisatie_land1"] for row in df.iterrows()]
        df["venue"] = [row[1]["venue_naam"] if row[1]["venue_naam"] else row[1]["organisatie_naam"] for row in df.iterrows()]
        df["source"] = ["datakunstenbe"] * len(df.index)
        df["artiest_mb_naam"] = df["artiest"]
        self.concerts = df.drop(labels=["id", "year", "month", "day", "organisatie_naam", "organisatie_stad1", "organisatie_land1", "organisatie_stad2", "organisatie_land2", "organisatie_stad3", "organisatie_land3", "venue_naam", "venue_stad", "venue_land"], axis=1)


class DriveSyncer(object):
    def __init__(self):
        # connect with google drive
        gauth = GoogleAuth()
        gauth.LoadCredentialsFile("resources/credentials.json")
        gauth.SaveCredentialsFile("resources/credentials.json")
        self.drive = GoogleDrive(gauth)
        self.resource_files_gdrive_ids = [
            ("belgian_mscbrnz_artists.xlsx", "1tKXyj_fySMTlAv0w4JVPDa7I0LzRY-5gx5Cf1pnG76A"),
            ("city_cleaning.xlsx", "12Ad6Yony5mvYVKHZQe4dRkod3jUgGd2xKVjJZcb-75Q"),
            ("country_cleaning.xlsx", "1HCVWAGLPrT572bZNbJLNhEd4qckF65x6lmYTTLf93dE"),
            ("ignore_list.xlsx", "1YeB7NcqFqU7Cnd_WcyeA8a7b-MrOe07uv18p_2Qovio"),
            ("manual.xlsx", "1OxF3zHB2FeM6PKasJLdPLzgFKM8gV_oj7-NlcjQJuow"),
            ("merge_artists.xlsx", "1su7MRynSZO9T1RTcH9hvJ_Gafudb58jREmHm01_n80k")
        ]

    def downstream(self):
        for item in self.resource_files_gdrive_ids:
            self.update_local_resource(item[0], item[1])
        self.update_local_latest()

    def upstream(self):
        for item in self.resource_files_gdrive_ids:
            self.update_remote_resource(item[0], item[1])
        self.update_remote_latest()

    def update_local_resource(self, filename, fid):
        file = self.drive.CreateFile({"id": fid})
        file.GetContentFile("resources/{0}".format(filename), mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    def update_local_latest(self):
        file = self.drive.CreateFile({"id": "0B4I3gofjeGMHWUk5bmZvQm5fdFk"})
        file.GetContentFile("output/latest.xlsx", mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    def update_remote_resource(self, filename, fid):
        file = self.drive.CreateFile({'title': filename, 'id': fid})
        file.SetContentFile("resources/" + filename)
        file['parents'] = [{"kind": "drive#fileLink", "id": '0B4I3gofjeGMHRFhZbzNzLTJCQU0'}]
        file.Upload(param={"convert": True})

    def update_remote_latest(self):
        file = self.drive.CreateFile({'title': "latest.xlsx", 'id': "0B4I3gofjeGMHWUk5bmZvQm5fdFk"})
        file.SetContentFile("output/latest.xlsx")
        file['parents'] = [{"kind": "drive#fileLink", "id": '0B4I3gofjeGMHRFhZbzNzLTJCQU0'}]
        file.Upload(param={"convert": True})


class Grabber(object):
    def __init__(self, update_from_musicbrainz=True):
        self.syncdrive = DriveSyncer()
        self.mbab = MusicBrainzArtistsBelgium(update=update_from_musicbrainz)
        self.songkickleecher = SongkickLeecher()
        self.bandsintownleecher = BandsInTownLeecher()
        self.setlistleecher = SetlistFmLeecher()
        self.facebookleecher = FacebookEventLeecher()
        self.current = None
        self.df = None
        self.diff = None

    def grab(self):
        """
        overview method to run all the grabbing
        :return:
        """
        self.syncdrive.downstream()
        self.load_artists()
        self.leech([self.songkickleecher, self.bandsintownleecher, self.setlistleecher, self.facebookleecher])
        self.add_manual_concerts()
        self.add_datakunstenbe_concerts()
        self.update_previous_version()
        self.make_concerts()
        self.infer_cancellations()
        self.persist_output()
        self.syncdrive.upstream()

    def load_artists(self):
        """
        go through belgian musicbrainz artists to fetch a list of songkick, bandsintown, setlist.fm, facebook ... urls
        :return:
        """
        self.mbab.load_list()

    def leech(self, leechers):
        """
        leech all the platforms, and combine the found events in a pandas dataframe
        :param leechers: a list of PlatformLeechers instances
        :return:
        """
        for leecher in leechers:
            leecher.set_platform_identifiers()
            leecher.set_events_for_identifiers()
        lines = sum([leecher.events for leecher in leechers], [])
        self.current = DataFrame(lines)

    def add_manual_concerts(self):
        manual = read_excel("resources/manual.xlsx")
        manual["datum"] = [datum.date() for datum in to_datetime(manual["datum"].values)]
        manual["source"] = ["manual"] * len(manual.index)
        self.current = manual.append(self.current, ignore_index=True)

    def add_datakunstenbe_concerts(self):
        dkbc = DataKunstenBeConnector()
        dkbc.get_concerts_abroad()
        self.current = self.current.append(dkbc.concerts, ignore_index=True)

    def update_previous_version(self):
        # fix weird symbols in columns
        for column in ["titel", "artiest", "venue", "artiest", "stad", "land"]:
            self.current[column] = self.current[column].map(
                lambda x: ''.join([str(c) for c in str(x) if ord(str(c)) > 31 or ord(str(c)) == 9]))

        # add a last_seen_on column to current dataset
        self.current["last_seen_on"] = [datetime.now().date()] * len(self.current.index)

        # read in the previously found concerts
        previous = read_excel("output/latest.xlsx")

        # do some fixes: make sure dates are dates, remove previous concert ids and
        # the cleaning of the cities and countries
        previous["datum"] = [datum.date() if datum is not None else None for datum in previous["datum"]]
        previous.drop("concert_id", 1, inplace=True)
        previous.drop("stad_clean", 1, inplace=True)
        previous.drop("land_clean", 1, inplace=True)

        # combine the previous dataset with the current dataset
        self.df = previous.append(self.current, ignore_index=True)

        # fix the dates of last seen on
        self.df["last_seen_on"] = [date.date() for date in self.df["last_seen_on"]]

        # remove duplicates on the basis of the event_id, and keep the first, which is the oldest observation
        # because of this, we keep potential corrections in earlier versions.
        self.df.drop_duplicates(subset=["event_id"], keep="first", inplace=True)

        # add a genre to each concert based on the artist
        self.df["maingenre"] = [self.mbab.maingenres[mbid] if mbid in self.mbab.maingenres else "Rest"
                                for mbid in self.df["artiest_mb_id"]]

        # update date of previous events that are also seen currently
        updated_event_ids = previous[previous["event_id"].isin(self.current["event_id"])]["event_id"].values
        updated_event_ids_index = self.df[self.df["event_id"].isin(updated_event_ids)].index
        for event_id in updated_event_ids_index:
            self.df.set_value(event_id, "last_seen_on", datetime.now().date())

        # extract the newly added concerts that did not yet appear in the previous version
        # to give an overview of additions
        self.diff = self.current[-self.current["event_id"].isin(previous["event_id"])]
        self.diff.to_excel("output/diff_" + str(datetime.now().date()) + ".xlsx")

    def clean_country_names(self):
        # resolve full country names to iso code
        clean_countries = []
        country_cleaning = read_excel("resources/country_cleaning.xlsx")
        country_cleaning_additions = set()
        for land in self.df["land"]:
            land = str(land).strip()
            if len(land) == 2:
                clean_country = "UK" if land == "GB" else land
                clean_countries.append(clean_country.upper())
            else:
                try:
                    clean_country = countries.get(name=land).alpha_2
                    clean_country = "UK" if clean_country == "GB" else clean_country
                except KeyError:
                    if land in country_cleaning["original"].values:
                        clean_country = country_cleaning[country_cleaning["original"] == land]["clean"].iloc[0]
                    else:
                        country_cleaning_additions.add(land)
                        clean_country = None
                clean_countries.append(clean_country)
        country_cleaning.append(DataFrame([{"original": land, "clean": None} for land in country_cleaning_additions]), ignore_index=True).drop_duplicates().to_excel("resources/country_cleaning.xlsx")
        self.df["land_clean"] = clean_countries

    def clean_city_names(self):
        # resolve dirty city names to clean city names
        city_cleaning_additions = set()
        clean_cities = []
        city_cleaning = read_excel("resources/city_cleaning.xlsx")
        for stad in self.df["stad"]:
            if stad in city_cleaning["original"].values:
                clean_city = city_cleaning[city_cleaning["original"] == stad]["clean"].iloc[0]
            else:
                city_cleaning_additions.add(stad)
                clean_city = None
            clean_cities.append(clean_city)
        city_cleaning.append(DataFrame([{"original": stad, "clean": None} for stad in city_cleaning_additions]), ignore_index=True).drop_duplicates().to_excel("resources/city_cleaning.xlsx")
        self.df["stad_clean"] = clean_cities

    def handle_ambiguous_artists(self):
        # for newly added events, add a merge name, so that the old corrections in merge names remain
        merge_artists_xlsx = read_excel("resources/merge_artists.xlsx")
        merge_artists = {row[1]["original"]: row[1]["clean"] for row in merge_artists_xlsx.iterrows()}
        for event_id in self.diff["event_id"]:
            df_index = self.df[self.df["event_id"] == event_id].index[0]
            artiest_mb_naam = self.df.loc[df_index, "artiest_mb_naam"]
            self.df.set_value(df_index, "artiest_merge_naam", merge_artists[artiest_mb_naam] if artiest_mb_naam in merge_artists else artiest_mb_naam)

    def make_concerts(self):
        # make a concert_id
        artist_date_city_triples = set([tuple(x) for x in self.df[["artiest_merge_naam", "datum", "stad_clean"]].values])
        gig_triple_id = 0
        for gig_triple in artist_date_city_triples:
            events = self.df[(self.df["artiest_merge_naam"] == gig_triple[0]) & (self.df["datum"] == gig_triple[1]) & (self.df["stad_clean"] == gig_triple[2])]
            for i in events.index:
                self.df.set_value(i, "concert_id", gig_triple_id)
            gig_triple_id += 1

        # per concert id, mark songkick/bit/setlist/facebook (in that order) as "to show"
        for concert_id in self.df["concert_id"].unique():
            concerts = self.df[self.df["concert_id"] == concert_id]
            if "manual" in concerts["source"].values:
                self.df.set_value(concerts[concerts["source"] == "manual"].index[0], "visible", True)
            elif "datakunstenbe" in concerts["source"].values:
                self.df.set_value(concerts[concerts["source"] == "datakunstenbe"].index[0], "visible", True)
            elif "songkick" in concerts["source"].values:
                self.df.set_value(concerts[concerts["source"] == "songkick"].index[0], "visible", True)
            elif "bandsintown" in concerts["source"].values:
                self.df.set_value(concerts[concerts["source"] == "bandsintown"].index[0], "visible", True)
            elif "setlist" in concerts["source"].values:
                self.df.set_value(concerts[concerts["source"] == "setlist"].index[0], "visible", True)
            elif "facebook" in concerts["source"].values:
                self.df.set_value(concerts[concerts["source"] == "facebook"].index[0], "visible", True)
        self.df["visible"].fillna(False, inplace=True)
        self.df["ignore"].fillna(False, inplace=True)

    def infer_cancellations(self):
        self.df["cancelled"] = (self.df["datum"] > datetime.now()) & (self.df["last_seen_on"] < datetime.now())

    def persist_output(self):
        self.df.to_excel("output/latest.xlsx")


if __name__ == "__main__":
    grabber = Grabber(update_from_musicbrainz=False)
    grabber.grab()
