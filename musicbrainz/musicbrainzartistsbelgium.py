from musicbrainzngs import set_useragent, search_artists, get_area_by_id, musicbrainz, get_artist_by_id
from pandas import read_excel, DataFrame
from re import sub
from time import sleep
from datetime import datetime

class MusicBrainzArtistsBelgium(object):
    def __init__(self, update=False):
        self.update = update
        self.aantal_concerten_per_mbid = None
        set_useragent("kunstenpunt", "0.1", "github.com/kunstenpunt")
        self.lijst = None
        self.genres = {}

    def calculate_concerts_abroad(self):
        concerts = read_excel("./output/latest.xlsx")
        concerts_abroad_future = concerts[-((concerts["land_clean"] == "Belgium") | (concerts["land_clean"].isnull())) & (concerts["datum"] >= datetime(2010, 1, 1))]
        self.aantal_concerten_per_mbid = concerts_abroad_future.groupby(["artiest_mb_id"])["event_id"].count()

    def make_genre_mapping(self):
        for row in self.lijst.iterrows():
            key = row[1]["mbid"]
            value = row[1]["maingenre"]
            self.genres[key] = value

    def load_list(self):
        self.lijst = read_excel("./resources/belgian_mscbrnz_artists.xlsx")

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
    def __get_drop_url(artist, domain):
        urls = []
        if "url-relation-list" in artist:
            for url in artist["url-relation-list"]:
                if domain in url["target"]:
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
            except musicbrainz.ResponseError:
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
                artists_area = search_artists(area=area, beginarea=area, endarea=area, limit=limit, offset=offset)
                artists['artist-list'] = artists_area["artist-list"]
                artists['artist-count'] = artists_area["artist-count"]
            except musicbrainz.NetworkError:
                sleep(25.0)
            except musicbrainz.ResponseError:
                sleep(25.0)
        return artists

    def __number_of_concerts(self, mbid):
        try:
            return self.aantal_concerten_per_mbid.loc[mbid]
        except KeyError:
            return 0

    @staticmethod
    def __is_on_ignore_list(mbid):
        ignore_list = read_excel("./resources/ignore_list.xlsx")
        return mbid in ignore_list["mbid"]

    def _obtain_a_specific_mb_artist(self, mbid):
        artist = None
        while artist is None:
            try:
                sleep(1.0)
                artist = get_artist_by_id(mbid, includes=["url-rels"])["artist"]
            except musicbrainz.NetworkError as e:
                print("musicbrainz netwerkerror", e)
                sleep(25.0)
            except musicbrainz.Response:
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
                    if ("area" in hit and hit["area"]["id"] == area_id[0]) or ("begin-area" in hit and hit["begin-area"]["id"] == area_id[0]) or ("end-area" in hit and hit["end-area"]["id"] == area_id[0]):
                        lijn = self._obtain_a_specific_mb_artist(hit["id"])
                        belgium.append(lijn)
                offset += limit
                total_search_results = search_results["artist-count"]

        for mbid in read_excel("./resources/grace_list.xlsx")["mbid"].values:
            lijn = self._obtain_a_specific_mb_artist(mbid)
            belgium.append(lijn)

        self.lijst = DataFrame(belgium).drop_duplicates(subset="mbid")
        self.lijst.to_excel("./resources/belgian_mscbrnz_artists.xlsx")

    def mb_lijn(self, hit):
        return {
            "band": hit["name"],
            "mbid": hit["id"],
            "area": hit["area"]["name"] if "area" in hit else None,
            "begin-area": hit["begin-area"]["name"] if "begin-area" in hit else None,
            "end-area": hit["end-area"]["name"] if "end-area" in hit else None,
            "begin": hit["life-span"]['begin'] if "life-span" in hit and "begin" in hit["life-span"] else None,
            "end": hit["life-span"]["end"] if "life-span" in hit and "end" in hit["life-span"] else None,
            "ended": hit["life-span"]["ended"] if "life-span" in hit and "ended" in hit["life-span"] else None,
            "disambiguation": hit["disambiguation"] if "disambiguation" in hit else None,
            "facebook": str(self.__get_rel_url(hit, "social network", "facebook.com")),
            "songkick": str(self.__get_rel_url(hit, "songkick")),
            "bandsintown": str(self.__get_rel_url(hit, "bandsintown")),
            "setlist": str(self.__get_rel_url(hit, "setlistfm")),
            "spotify": str(self.__get_drop_url(hit, "spotify")),
            "bandcamp": str(self.__get_drop_url(hit, "bandcamp")),
            "itunes": str(self.__get_drop_url(hit, "itunes")),
            "soundcloud": str(self.__get_drop_url(hit, "soundcloud")),
            "deezer": str(self.__get_drop_url(hit, "deezer")),
            "youtube": str(self.__get_drop_url(hit, "youtube")),
            "number_of_concerts": self.__number_of_concerts(hit["id"]),
            "on_ignore_list": self.__is_on_ignore_list(hit["id"]),
            "maingenre": self.genres[hit["id"]] if hit["id"] in self.genres else None
        }


if __name__ == "__main__":
    mbab = MusicBrainzArtistsBelgium()
    mbab.calculate_concerts_abroad()
    mbab.load_list()
    mbab.make_genre_mapping()
    mbab.update_list()
