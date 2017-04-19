from re import sub
from musicbrainzngs import set_useragent, search_artists, get_area_by_id, musicbrainz, get_artist_by_id
from time import sleep
from codecs import open

set_useragent("kunstenpunt", "0.1", "github.com/kunstenpunt")


def get_land(artist):
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


def get_rel_url(artist, urltype):
    if "url-relation-list" in artist["artist"]:
        for url in artist["artist"]["url-relation-list"]:
            if url["type"] == urltype:
                return url["target"]


def make_artist_name(n):
    return sub(r"[^\w\s\d]", "", sub(r"[\[\(].+?[\)\]]", "", n)).strip()


def get_parts_of(area_id):
    part_of_ids = []
    areas = get_area_by_id(area_id, includes="area-rels")["area"]["area-relation-list"]
    for area in areas:
        if area["type"] == "part of" and "direction" not in area:
            part_of_ids.append((area["area"]["id"], area["area"]["name"]))
    return part_of_ids


def search_artists_in_area(area, limit, offset):
    artists = search_artists(area=area, limit=limit, offset=offset)
    return artists


def get_musicbrainz_belgian_artist_ids(update=True):
    if update:
        area_ids = [("5b8a5ee5-0bb3-34cf-9a75-c27c44e341fc", "Belgium")]
        new_parts = get_parts_of(area_ids[0][0])
        area_ids.extend(new_parts)
        while len(new_parts) > 0:
            new_new_parts = []
            for new_part in new_parts:
                print("nieuwe locatie", new_part[1])
                if new_part[1] != "Wallonie":
                    parts = get_parts_of(new_part[0])
                    new_new_parts.extend(parts)
                    area_ids.extend(parts)
            new_parts = new_new_parts
        belgium = []
        for area_id in area_ids:
            offset = 0
            limit = 100
            area_hits = []
            total_search_results = 1
            while offset < total_search_results:
                search_results = search_artists_in_area(area_id[1], limit, offset)
                for hit in search_results["artist-list"]:
                    if hit["area"]["id"] == area_id[0]:
                        artist = get_artist_by_id(hit["id"], includes=["url-rels"])
                        songkick_url = get_rel_url(artist, "songkick")
                        bandsintown_url = get_rel_url(artist, "bandsintown")
                        lijn = "\t".join(
                            [
                                hit["name"],
                                hit["id"],
                                hit["area"]["name"],
                                str(songkick_url),
                                str(bandsintown_url)
                            ]
                        )
                        print(lijn)
                        area_hits.append(lijn)
                offset += limit
                total_search_results = search_results["artist-count"]
            print(len(area_hits))
            belgium.extend(area_hits)
        with open("resources/belgian_mscbrnz_artists.lst", "w", "utf-8") as f:
            f.write("\n".join(belgium))
    else:
        with open("resources/belgian_mscbrnz_artists.lst", "r", "utf-8") as f:
            belgium = f.read().split("\n")
    return belgium
