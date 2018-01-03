from pandas import read_excel, DataFrame, to_datetime, isnull, Timestamp
from os import remove
import sys
from re import sub
from musicbrainzngs import set_useragent, search_artists, get_area_by_id, musicbrainz, get_artist_by_id
from codecs import open
from time import sleep
from json import loads, dump, load, decoder
from requests import get, exceptions
from math import ceil
from datetime import datetime, timedelta, date
import bandsintown
from urllib import parse as urlparse
import facebook
from dateparser import parse as dateparse
import psycopg2
from configparser import ConfigParser
from pycountry import countries
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from resources.sendmail.sendmessage import sendmail
from requests import post
import hashlib
import hmac
import binascii
from json import dumps
import geojson


class PlatformLeecher(object):
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
                if not isnull(row[self.platform]) and row["band"] not in bands_done:
                    self.platform_identifiers.append((row[["band", "mbid", self.platform]]))
                else:
                    bands_done.add(row["band"])
            else:
                print("ignoring", row["band"])

    def set_events_for_identifiers(self):
        for band, mbid, urls in self.platform_identifiers:
            for url in urls.split(","):
                print(self.platform, band, url)
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
        html = get(url).text
        try:
            json_response = loads(html) if html is not None else {}
        except decoder.JSONDecodeError:
            json_response = {}
        if "resultsPage" in json_response:
            resultspaga = json_response["resultsPage"]
            amount_events = resultspaga["totalEntries"] if "totalEntries" in resultspaga else 0
            amount_pages = ceil(amount_events / 50.0)
            while page <= amount_pages:
                if json_response["resultsPage"]["status"] == "ok":
                    for event in json_response["resultsPage"]["results"]["event"]:
                        self.events.append(self.map_platform_to_schema(event, band, mbid, {"artist_id": artistid, "artist_name": artistname}))
                    page += 1
                    url = base_url.format(artistid, self.platform_access_granter, page)
                    html = get(url).text
                    try:
                        json_response = loads(html)
                    except decoder.JSONDecodeError:
                        print("decoder error")
                        json_response = {}

    def map_platform_to_schema(self, event, band, mbid, other):
        concertdate = Timestamp(dateparse(event["start"]["date"]).date())
        return {
            "titel": event["displayName"].strip().rstrip(concertdate.strftime("%B %d, %Y")),
            "titel_generated": event["displayName"].strip().rstrip(concertdate.strftime("%B %d, %Y")),
            "datum": concertdate,
            "einddatum": Timestamp(dateparse(event["end"]["date"]).date()) if "end" in event else None,
            "artiest": other["artist_name"],
            "artiest_id": "songkick_" + str(other["artist_id"]),
            "artiest_mb_naam": band,
            "artiest_mb_id": mbid,
            "stad": ",".join([i.strip() for i in event["location"]["city"].split(",")[0:-1]]),
            "land": event["location"]["city"].split(",")[-1].strip(),
            "venue": event["displayName"].strip() if event["type"] == "Festival" else event["venue"]["displayName"].strip(),
            "latitude": event["venue"]["lat"],
            "longitude": event["venue"]["lng"],
            "source": self.platform,
            "event_id": "songkick_" + str(event["id"]),
            "event_type": event["type"].lower()
        }


class BandsInTownLeecher(PlatformLeecher):
    def __init__(self):
        super().__init__()
        self.bitc = bandsintown.Client("kunstenpunt")
        self.platform = "bandsintown"

    def set_events_for_identifier(self, band, mbid, url):
        period = "1900-01-01,2050-01-01"
        bandnaam = urlparse.unquote(url.split("/")[-1].split("?came_from")[0])
        events = None
        while events is None:
            try:
                events = self.bitc.events(bandnaam, date=period)
                if events is not None:
                    while "errors" in events:
                        if "Rate limit exceeded" in events["errors"]:
                            print("one moment!")
                            sleep(60.0)
                            events = self.bitc.events(bandnaam, date=period)
                        else:
                            events = []
            except decoder.JSONDecodeError:
                print("decoder error")
                events = None

        for concert in events:
            self.events.append(self.map_platform_to_schema(concert, band, mbid, {}))

    def map_platform_to_schema(self, concert, band, mbid, other):
        region = concert["venue"]["region"] if "region" in concert["venue"] else None
        stad = (concert["venue"]["city"]).strip()
        if region is not None:
            stad = stad + ", " + region.strip()
        return {
            "datum": Timestamp(dateparse(concert["datetime"]).date()),
            "land": (concert["venue"]["country"]).strip(),
            "stad": stad,
            "venue": (concert["venue"]["place"]).strip(),
            "titel": (concert["title"]).strip(),
            "titel_generated": (concert["title"]).strip(),
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
        try:
            remove("resources/facebook_errors.txt")
        except FileNotFoundError:
            pass

    def set_events_for_identifier(self, band, mbid, url):
        page_label = url.split("/")[-1].split("-")[-1] if "-" in url.split("/")[-1] else url.split("/")[-1]
        try:
            events = self.graph.get_connections(id=page_label, connection_name="events")
            if "data" in events:
                for concert in events["data"]:
                    self.events.append(self.map_platform_to_schema(concert, band, mbid, {"page_label": page_label}))
        except facebook.GraphAPIError as e:
            with open("resources/facebook_errors.txt", "a") as f:
                f.write(datetime.now().date().isoformat() + "\t" + url + "\t" + str(e) + "\n")
        except exceptions.ConnectionError:
            self.set_events_for_identifier(band, mbid, url)

    def map_platform_to_schema(self, concert, band, mbid, other):
        venue = concert["place"]["name"] if "place" in concert else None
        stad = concert["place"]["location"]["city"] if "place" in concert and "location" in concert["place"] and "city" in concert["place"]["location"] else None
        state = concert["place"]["location"]["state"] if "place" in concert and "location" in concert["place"] and "state" in concert["place"]["location"] else None
        if state is not None and stad is not None:
            stad = stad + ", " + state
        land = concert["place"]["location"]["country"] if "place" in concert and "location" in concert["place"] and "country" in concert["place"]["location"] else None
        einddatum = Timestamp(dateparse(concert["end_time"]).date()) if "end_time" in concert else None
        return {
            "titel": concert["name"] if "name" in concert else None,
            "titel_generated": str(band) + " @ " + str(venue) + " in " + str(stad) + ", " + str(land),
            "datum": Timestamp(dateparse(concert["start_time"]).date()),
            "einddatum": einddatum,
            "event_type": "festival" if einddatum else None,
            "artiest": band,
            "artiest_id": "facebook_" + other["page_label"],
            "artiest_mb_naam": band,
            "artiest_mb_id": mbid,
            "stad": stad,
            "land": land,
            "venue": venue,
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
            try:
                response = loads(r.text)
            except decoder.JSONDecodeError:
                response = {}
            if "setlist" in response:
                for concert in response["setlist"]:
                    self.events.append(self.map_platform_to_schema(concert, band, mbid, {}))
                total_hits = int(response["total"])
                retrieved_hits += int(response["itemsPerPage"])
            else:
                total_hits = 0
            p += 1

    def map_platform_to_schema(self, concert, band, mbid, other):
        stad = concert["venue"]["city"]["name"]
        state = concert["venue"]["city"]["stateCode"] if "stateCode" in concert["venue"]["city"] else None
        if state is not None:
            stad = stad + ", " + state
        return {
            "titel": concert["info"] if "info" in concert else None,
            "titel_generated":  band + " @ " + concert["venue"]["name"] + " in " + concert["venue"]["city"]["name"] + ", " + concert["venue"]["city"]["country"]["code"],
            "datum": Timestamp(dateparse(concert["eventDate"], ["%d-%m-%Y"]).date()),
            "artiest": concert["artist"]["name"],
            "artiest_id": "setlist_" + concert["artist"]["url"],
            "artiest_mb_naam": band,
            "artiest_mb_id": mbid,
            "stad": stad,
            "land": concert["venue"]["city"]["country"]["code"],
            "venue": concert["venue"]["name"],
            "latitude": concert["venue"]["city"]["coords"]["lat"] if "lat" in concert["venue"]["city"]["coords"] else None,
            "longitude": concert["venue"]["city"]["coords"]["long"] if "long" in concert["venue"]["city"]["coords"] else None,
            "source": self.platform,
            "event_id": "setlist" + concert["id"]
        }


class MusicBrainzArtistsBelgium(object):
    def __init__(self, update=False):
        self.update = update
        self.aantal_concerten_per_mbid = None
        set_useragent("kunstenpunt", "0.1", "github.com/kunstenpunt")
        self.lijst = None
        self.genres = {}

    def calculate_concerts_abroad(self):
        concerts = read_excel("output/latest.xlsx")
        concerts_abroad_future = concerts[(-concerts["land_clean"].isin(["Belgium", None, "Unknown", ""])) & (concerts["datum"] > datetime.now())]
        self.aantal_concerten_per_mbid = concerts_abroad_future.groupby(["artiest_mb_id"])["event_id"].count()

    def make_genre_mapping(self):
        for row in self.lijst.iterrows():
            key = row[1]["mbid"]
            value = row[1]["maingenre"]
            self.genres[key] = value

    def load_list(self):
        self.lijst = read_excel("resources/belgian_mscbrnz_artists.xlsx")

    # TODO make a list of musicbrainz ids that refer to the same platform url, e.g. toots thielemans and toots
    # TODO thielemans quartet both refer to the same songkick url; store these musicbrainz ids in the object so that
    # TODO the ids can be used to detect duplicate concerts

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
        urls = []
        if "url-relation-list" in artist:
            for url in artist["url-relation-list"]:
                if url["type"] == urltype:
                    if domain:
                        if domain in url["target"]:
                            urls.append(url["target"])
                    else:
                        urls.append(url["target"])
        return ",".join(urls)

    @staticmethod
    def __make_artist_name(artist_name):
        return sub(r"[^\w\s\d]", "", sub(r"[\[\(].+?[\)\]]", "", artist_name)).strip()

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

    @staticmethod
    def __is_on_ignore_list(mbid):
        ignore_list = read_excel("resources/ignore_list.xlsx")
        return mbid in ignore_list["mbid"]

    def _obtain_a_specific_mb_artist(self, mbid):
        artist = None
        while artist is None:
            try:
                sleep(1.0)
                artist = get_artist_by_id(mbid, includes=["url-rels"])["artist"]
            except musicbrainz.NetworkError:
                sleep(25.0)
        print("adding", artist["name"])
        return self.mb_lijn(artist)

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
            total_search_results = 1
            while offset < total_search_results:
                search_results = self.__search_artists_in_area(area_id[1], limit, offset)
                for hit in list(search_results["artist-list"]):
                    if ("area" in hit and hit["area"]["id"] == area_id[0]) or ("begin-area" in hit and hit["begin-area"]["id"] == area_id[0]):
                        lijn = self._obtain_a_specific_mb_artist(hit["id"])
                        belgium.append(lijn)
                offset += limit
                total_search_results = search_results["artist-count"]

        for mbid in read_excel("resources/grace_list.xlsx")["mbid"].values:
            lijn = self._obtain_a_specific_mb_artist(mbid)
            belgium.append(lijn)

        self.lijst = DataFrame(belgium).drop_duplicates(subset="mbid")
        self.lijst.to_excel("resources/belgian_mscbrnz_artists.xlsx")

    def mb_lijn(self, hit):
        return {
            "band": hit["name"],
            "mbid": hit["id"],
            "area": hit["area"]["name"] if "area" in hit else None,
            "begin-area": hit["begin-area"]["name"] if "begin-area" in hit else None,
            "begin": hit["life-span"]['begin'] if "life-span" in hit and "begin" in hit["life-span"] else None,
            "end": hit["life-span"]["end"] if "life-span" in hit and "end" in hit["life-span"] else None,
            "ended": hit["life-span"]["ended"] if "life-span" in hit and "ended" in hit["life-span"] else None,
            "disambiguation": hit["disambiguation"] if "disambiguation" in hit else None,
            "facebook": str(self.__get_rel_url(hit, "social network", "facebook.com")),
            "songkick": str(self.__get_rel_url(hit, "songkick")),
            "bandsintown": str(self.__get_rel_url(hit, "bandsintown")),
            "setlist": str(self.__get_rel_url(hit, "setlistfm")),
            "number_of_concerts": self.__number_of_concerts(hit["id"]),
            "on_ignore_list": self.__is_on_ignore_list(hit["id"]),
            "maingenre": self.genres[hit["id"]] if hit["id"] in self.genres else None
        }


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
        df["datum"] = [Timestamp(datetime(int(row[1]["year"]), int(row[1]["month"]), int(row[1]["day"]) if row[1]["day"] > 0 else 1).date()) for row in df.iterrows()]
        df["stad"] = [row[1]["venue_stad"] if row[1]["venue_stad"] is not None else row[1]["organisatie_stad3"] if row[1]["organisatie_stad3"] is not None else row[1]["organisatie_stad2"] if row[1]["organisatie_stad2"] is not None else row[1]["organisatie_stad1"] for row in df.iterrows()]
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
        self.resource_files = [
            "belgian_mscbrnz_artists.xlsx",
            "city_cleaning.xlsx",
            "country_cleaning.xlsx",
            "venue_cleaning.xlsx",
            "ignore_list.xlsx",
            "grace_list.xlsx",
            "manual.xlsx",
            "merge_artists.xlsx"
        ]
        self.resource_files_gdrive_ids = None

    def set_resource_files_ids(self):
        self.resource_files_gdrive_ids = [(resource_file, self.get_google_drive_id(resource_file)) for resource_file in self.resource_files]

    def get_google_drive_id(self, filename):
        print("\t", filename)
        return self.drive.ListFile({'q': "'0B4I3gofjeGMHRFhZbzNzLTJCQU0' in parents and title='{0}' and trashed=false".format(filename)}).GetList()[0]["id"]

    def get_google_drive_link(self, filename):
        return self.drive.ListFile({'q': "'0B4I3gofjeGMHRFhZbzNzLTJCQU0' in parents and title='{0}' and trashed=false".format(filename)}).GetList()[0]["alternateLink"]

    def upload_file_to_leeches(self, filename, fid):
        file = self.drive.CreateFile({'title': filename, 'id': fid})
        file.SetContentFile(filename)
        file['parents'] = [{"kind": "drive#fileLink", "id": '0B4I3gofjeGMHRFhZbzNzLTJCQU0'}]
        file.Upload(param={"convert": True})

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


class Reporter(object):
    def __init__(self, now, drive):
        with open("output/report.json", "r", "utf-8") as f:
            self.previous_report = load(f)

        self.drive = drive
        self.aantal_musicbrainz_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre = None
        self.aantal_nieuwe_concerten = None
        self.old_status_ignore = None
        self.old_status_musicbrainz = None
        self.old_status_city_cleaning = None
        self.old_status_country_cleaning = None
        self.current_status_ignore = None
        self.current_status_city_cleaning = None
        self.current_status_country_cleaning = None
        self.current_status_musicbrainz = None
        self.aantal_ongecleande_steden = None
        self.aantal_ongecleande_landen = None
        self.report = None
        self.datum_vorige_check = None
        self.datum_recentste_check = now

    def set_aantal_musicbrainz_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre(self):
        has_concerts = self.current_status_musicbrainz["number_of_concerts"] > 0
        genre_is_rest = self.current_status_musicbrainz["maingenre"].isnull()
        aantal = len(self.current_status_musicbrainz[has_concerts & genre_is_rest].index)
        self.aantal_musicbrainz_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre = aantal

    def set_aantal_nieuwe_concerten(self):
        self.aantal_nieuwe_concerten = len(read_excel("output/diff_" + str(self.datum_recentste_check.date()) + ".xlsx").index)

    def take_snapshot_of_status(self, timing):
        if timing == "old":
            self.old_status_ignore = read_excel("resources/ignore_list.xlsx")
            self.old_status_musicbrainz = read_excel("resources/belgian_mscbrnz_artists.xlsx")
            self.old_status_city_cleaning = read_excel("resources/city_cleaning.xlsx")
            self.old_status_country_cleaning = read_excel("resources/country_cleaning.xlsx")
        if timing == "current":
            self.current_status_ignore = read_excel("resources/ignore_list.xlsx")
            self.current_status_city_cleaning = read_excel("resources/city_cleaning.xlsx")
            self.current_status_country_cleaning = read_excel("resources/country_cleaning.xlsx")
            self.current_status_musicbrainz = read_excel("resources/belgian_mscbrnz_artists.xlsx")

    def compare_current_with_old_status(self):
        self.set_datum_vorige_check()
        self.compare_city_cleaning()
        self.compare_country_cleaning()
        self.compare_musicbrainz()

    def compare_city_cleaning(self):
        empty_cleaning = self.current_status_city_cleaning["clean"].isnull()
        self.aantal_ongecleande_steden = len(self.current_status_city_cleaning[empty_cleaning].index)

    def compare_country_cleaning(self):
        empty_cleaning = self.current_status_country_cleaning["clean"].isnull()
        self.aantal_ongecleande_landen = len(self.current_status_country_cleaning[empty_cleaning].index)

    def compare_musicbrainz(self):
        appended = self.current_status_musicbrainz.append(self.old_status_musicbrainz)
        appended.drop_duplicates(keep=False, inplace=True)
        appended.to_excel("resources/belgian_mscbrnz_artists_diff.xlsx")
        fid = self.drive.get_google_drive_id("belgian_mscbrnz_artists_diff.xlsx")
        self.drive.upload_file_to_leeches("resources/belgian_mscbrnz_artists_diff.xlsx", fid)

    def do(self):
        self.set_aantal_musicbrainz_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre()
        self.compare_current_with_old_status()
        self.set_aantal_nieuwe_concerten()
        self.generate_report()
        self.send_report()

    def generate_report(self):
        self.report = {
            "recentste_check": self.datum_recentste_check.isoformat(),
            "vorige_check": self.datum_vorige_check,
            "steden": {
                "aantal_ongecleande_steden": self.aantal_ongecleande_steden,
                "link_city_cleaning": self.drive.get_google_drive_link("city_cleaning.xlsx")
            },
            "landen": {
                "aantal_ongecleande_landen": self.aantal_ongecleande_landen,
                "link_country_cleaning": self.drive.get_google_drive_link("country_cleaning.xlsx")
            },
            "concerten": {
                "aantal_nieuwe_concerten": self.aantal_nieuwe_concerten,
                "link_concerten": self.drive.get_google_drive_link("latest.xlsx")
            },
            "genres": {
                "aantal_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre": self.aantal_musicbrainz_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre,
                "link_mb_belgen": self.drive.get_google_drive_link("belgian_mscbrnz_artists_diff.xlsx")
            },
            "musicbrainz": {
                "totaal_aantal_mb_belgen": len(self.current_status_musicbrainz.index),
                "vorig_aantal_onderzochte_mb_belgen": len(self.old_status_musicbrainz.index),
                "aantal_nieuwe_mb_belgen": len(self.current_status_musicbrainz.index) - len(self.old_status_musicbrainz.index),
                "link_mb_belgen_diff": self.drive.get_google_drive_link("belgian_mscbrnz_artists_diff.xlsx"),
                "link_mb_belgen": self.drive.get_google_drive_link("belgian_mscbrnz_artists_diff.xlsx")
            },
            "ignore_list": {
                "totaal_aantal_genegeerde_mb_belgen": len(self.current_status_ignore.index),
                "vorig_aantal_genegeerd_mb_belgen": len(self.old_status_ignore.index),
                "link_genegeerd_mb_belgen": self.drive.get_google_drive_link("ignore_list.xlsx")
            }
        }

    def send_report(self):
        with open("output/report.json", "w", "utf-8") as f:
            dump(self.report, f, indent=2)
        with open("resources/sendmail/template.mstch", "r", "utf-8") as f:
            template = f.read()
        sendmail(self.report, template)

    def set_datum_vorige_check(self):
        try:
            self.datum_vorige_check = self.previous_report["recentste_check"]
        except TypeError:
            self.datum_vorige_check = "Not available"


class Grabber(object):
    def __init__(self, update_from_musicbrainz=True):
        self.syncdrive = DriveSyncer()
        self.now = datetime.now()
        self.reporter = Reporter(self.now, self.syncdrive)
        self.mbab = MusicBrainzArtistsBelgium(update=update_from_musicbrainz)
        self.songkickleecher = SongkickLeecher()
        self.bandsintownleecher = BandsInTownLeecher()
        self.setlistleecher = SetlistFmLeecher()
        self.facebookleecher = FacebookEventLeecher()
        self.previous = None
        self.current = None
        self.df = None
        self.diff = None
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
        self.add_manual_concerts()
        print("adding podiumfestivalinfo concerts")
        self.add_podiumfestivalinfo_concerts()

        print("adding data.kunsten.be concerts")
        self.add_datakunstenbe_concerts()

        self.update_previous_version()
        self.clean_country_names()
        self._clean_names("stad", "stad_clean", "resources/city_cleaning.xlsx")
        self._clean_names("venue", "venue_clean", "resources/venue_cleaning.xlsx")
        self.handle_ambiguous_artists()
        self.make_concerts()
        self.infer_cancellations()
        self._set_source_outlinks_per_concert()
        self.send_data_to_mr_henry()
        self.persist_output()
        self.reporter.take_snapshot_of_status("current")
        self.syncdrive.upstream()
        self.reporter.do()

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
        self.current = self.current.append(manual, ignore_index=True)

    def add_podiumfestivalinfo_concerts(self):
        pci = read_excel("resources/podiumfestivalinfo.xlsx")
        pci["datum"] = [Timestamp(datum.date()) for datum in to_datetime(pci["datum"].values)]
        pci["einddatum"] = [Timestamp(datum.date()) if not isnull(datum) else None for datum in to_datetime(pci["einddatum"].values)]
        pci["artiest_mb_id"] = [self._match_artist_name_to_mbid(artiest_mb_naam) for artiest_mb_naam in pci["artiest_mb_naam"].values]
        self.current = self.current.append(pci, ignore_index=True)

    def _match_artist_name_to_mbid(self, artiest_mb_naam):
        matches = self.mbab.lijst[self.mbab.lijst["band"] == artiest_mb_naam]["mbid"].values
        return matches[0] if len(matches) > 0 else None

    def add_datakunstenbe_concerts(self):
        dkbc = DataKunstenBeConnector()
        dkbc.get_concerts_abroad()
        self.current = self.current.append(dkbc.concerts, ignore_index=True)

    @staticmethod
    def _fix_weird_symbols(x):
        return ''.join([str(c) for c in str(x) if ord(str(c)) > 31 or ord(str(c)) == 9])

    def _fix_weird_symbols_in_columns(self, columns):
        for column in columns:
            self.current[column] = self.current[column].map(lambda x: self._fix_weird_symbols(x))

    def update_previous_version(self):
        print("update previous version")
        print("\tfixing weird symbols")
        self._fix_weird_symbols_in_columns(["titel", "artiest", "venue", "artiest", "stad", "land"])

        print("\tadding a last seen on column")
        self.current["last_seen_on"] = [self.now.date()] * len(self.current.index)

        print("\treading in the previous concerts")
        self.previous = read_excel("output/latest.xlsx")

        print("\tfixing dates and enddates, and dropping concert ids, clean cities, clean countries, visibility")
        for column in ["concert_id", "stad_clean", "land_clean", "iso_code_clean", "venue_clean", "visible", "source_0", "source_link_0", "source_1", "source_link_1", "source_2", "source_link_2", "source_3", "source_link_3", "source_4", "source_link_4"]:
            try:
                self.previous.drop(column, 1, inplace=True)
            except ValueError:
                continue

        print("\tcombing the two datasets")
        self.df = self.previous.append(self.current, ignore_index=True)

        print("\tfixing the last seen on date")
        self.df["last_seen_on"] = [lso.date() if isinstance(lso, datetime) else lso for lso in self.df["last_seen_on"]]

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

        print("\tgenerating a diff, and writing it to file")
        self._generate_diff()
        self.diff.to_excel("output/diff_" + str(self.now.date()) + ".xlsx")

    def send_data_to_mr_henry(self, test=False):
        df_filtered = self.df[(self.df["iso_code_clean"] != "BE")]
        for record in df_filtered.to_dict("records"):
            print(record)
            self._send_record_to_mr_henry_api(record, test=test)

    @staticmethod
    def json_serial(obj):
        if isinstance(obj, (datetime, date, Timestamp)):
            return obj.strftime("%Y/%m/%d")
        raise TypeError("Type %s not serializable" % type(obj))

    def _send_record_to_mr_henry_api(self, data, test=False):
        data = {key: data[key] for key in data if not isnull(data[key])}
        message = bytes(dumps(data, default=self.json_serial), "utf-8")
        with open("resources/mrhenrysecret.txt", "rb") as f:
            secret = bytes(f.read())

        signature = binascii.b2a_hex(hmac.new(secret, message, digestmod=hashlib.sha256).digest())

        base_url = "https://have-love-will-travel.herokuapp.com/"
        url = base_url + "import-json"

        params = {"signature": signature, "test": test}
        headers = {"Content-Type": "application/json"}

        r = post(url, data=message, params=params, headers=headers)
        if r.status_code != 200:
            print("issue with sending this record to the api", message, r.status_code, r.headers["X-Request-ID"])
        return r

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

    def _generate_diff(self):
        cur_not_in_prev = -self.current[["event_id", "artiest_mb_id"]].isin(self.previous[["event_id", "artiest_mb_id"]].to_dict(orient="list"))
        diff_indices = (cur_not_in_prev["event_id"] | cur_not_in_prev["artiest_mb_id"])
        self.diff = self.current[diff_indices]

    def _update_last_seen_on_dates_of_previous_events_that_are_still_current(self):
        prev_also_in_cur = self.previous[["event_id", "artiest_mb_id"]].isin(self.current[["event_id", "artiest_mb_id"]].to_dict(orient="list"))
        prev_indices = (prev_also_in_cur["event_id"] & prev_also_in_cur["artiest_mb_id"])
        events_artiesten = (self.previous["event_id"][prev_indices].values, self.previous["artiest_mb_id"][prev_indices].values)
        for i in range(0, len(events_artiesten[0])):
            event_id = events_artiesten[0][i]
            artiest_mb_id = events_artiesten[1][i]
            for idx in self.df[(self.df["event_id"] == event_id) & (self.df["artiest_mb_id"] == artiest_mb_id)].index:
                self.df.at[idx, "last_seen_on"] = self.now.date()

    @staticmethod
    def _convert_cleaned_country_name_to_full_name(twolettercode):
        try:
            return countries.get(alpha_2=twolettercode).name
        except KeyError:
            return None

    def clean_country_names(self):
        clean_countries = []
        clean_iso_codes = []
        country_cleaning = read_excel("resources/country_cleaning.xlsx")
        country_cleaning_additions = set()
        for land in self.df["land"]:
            land = str(land).strip()
            if len(land) == 2:
                clean_countries.append(self._convert_cleaned_country_name_to_full_name(land.upper()))
                clean_iso_codes.append(land.upper())
            else:
                try:
                    clean_country = countries.get(name=land).alpha_2
                except KeyError:
                    if land in country_cleaning["original"].values:
                        clean_country = country_cleaning[country_cleaning["original"] == land]["clean"].iloc[0]
                    else:
                        country_cleaning_additions.add(land)
                        clean_country = None
                clean_countries.append(self._convert_cleaned_country_name_to_full_name(clean_country))
                clean_iso_codes.append(clean_country)
        country_cleaning.append(DataFrame([{"original": land, "clean": None} for land in country_cleaning_additions]),
                                ignore_index=True).drop_duplicates().to_excel("resources/country_cleaning.xlsx")
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
        return True in [self.__inside_polygon(concert["longitude"], concert["latitude"], list(geojson.utils.coords(self.belgium[i]))) for i in range(0, len(self.belgium["features"]))]

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
            return "BandsInTown", "http://bandsintown.com/event/" + event_id.split("_")[-1]
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
            potential_source_values = ["manual", "datakunstenbe", "songkick", "bandsintown", "setlist", "facebook"]
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
