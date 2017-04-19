import bandsintown
from time import sleep
from musicbrainzngs import set_useragent
from datetime import datetime
from pandas import read_csv, DataFrame
import os
from glob import glob
from shared_code import get_musicbrainz_belgian_artist_ids


def check_if_artist_in_concert(band, concert, source):
    mbid = None
    for artist in concert["artists"]:
        if "mscbrnz" in source:
            if artist["name"] == band.split("\t")[0]:
                mbid = artist["mbid"] if artist["mbid"] is not None else "No mbid in bit"
        else:
            if artist["url"].lower() == str(band).split("/")[-1].split("?came_from")[0].lower():
                mbid = artist["mbid"] if artist["mbid"] is not None else "No mbid in bit"
    return mbid


groepen = read_csv("https://docs.google.com/spreadsheets/d/173K9EyJswhdSLZodGIjqFOU8wpUMYb_JOXmz2bebxDg/pub?gid=0&single=true&output=tsv", sep="\t")

bitc = bandsintown.Client("kunstenpunt")

set_useragent("kunstenpunt", "0.1", "github.com/kunstenpunt")

mscbrnz_cache = {}

tabel_nieuw = []
errors = []

bands = get_musicbrainz_belgian_artist_ids(update=False)
bands.extend(groepen["Bandsintown"].values)

for band in bands:
    events = []
    periode = "2013-01-01,2017-12-31"

    # from diane
    if "bandsintown.com" in str(band):
        bandnaam = band.split("/")[-1].split("?came_from")[0]
        bandid = band.split("/")[-1].split("?came_from")[0]
        print(bandnaam)
        events = bitc.events(bandnaam, date=periode)
        source = "bandsintown gold"
        while "errors" in events:
            if "Rate limit exceeded" in events["errors"]:
                print("one moment!")
                sleep(60.0)
                events = bitc.events(bandnaam, date=periode)
            else:
                events = []

    # from musicbrainz
    if len(str(band).split("\t")) > 2:
        bandnaam, bandid = str(band).split("\t")[0:2]
        print(bandnaam)
        events = bitc.events(mbid=bandid, date=periode)
        source = "bandsintown mscbrnz"
        while "errors" in events:
            if "Rate limit exceeded" in events["errors"]:
                print("one moment!")
                sleep(60.0)
                events = bitc.events(mbid=bandid, date=periode)
            else:
                events = []

    # go through found concerts
    if len(events) > 0:
        for concert in events:
            mscbrnz_id = check_if_artist_in_concert(band, concert, source)
            if mscbrnz_id:
                lijn = {
                    "datum": (datetime.strptime(concert["datetime"], "%Y-%m-%dT%H:%M:%S")).date(),
                    "land": (concert["venue"]["country"]).strip(),
                    "stad": (concert["venue"]["city"]).strip(),
                    "venue": (concert["venue"]["place"]).strip(),
                    "titel": (concert["title"]).strip(),
                    "artiest": bandnaam,
                    "mbid": mscbrnz_id,
                    "latitude": (concert["venue"]["latitude"]),
                    "longitude": (concert["venue"]["longitude"]),
                    "source": source
                }
                tabel_nieuw.append(lijn)
            else:
                errors.append({"source": source, "artiest": bandnaam, "id": bandid})

DataFrame(tabel_nieuw).to_excel("providers/bandsintown.xlsx", index=False)
if len(errors) > 0:
    DataFrame(errors).drop_duplicates(inplace=False).to_excel("bandsintown_errors.xlsx", index=False)
else:
    if "bandsintown_errors.xlsx" in glob("*.xlsx"):
        os.remove("bandsintown_errors.xlsx")
