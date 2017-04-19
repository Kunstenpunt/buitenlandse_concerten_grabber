from requests import get
from json import loads
from math import ceil
from pandas import DataFrame, read_csv
from musicbrainzngs import set_useragent
from datetime import datetime
from shared_code import get_musicbrainz_belgian_artist_ids

mscbrnz_cache = {}
lines = []

with open("resources/songkick_api_key.txt") as f:
    api_key = f.read()


def go_through_api_event_pages(base_url, artistname, artistid, source):
    page = 1
    url = base_url.format(artistid, api_key, page)
    json_response = loads(get(url).text)
    amount_events = json_response["resultsPage"]["totalEntries"] if "totalEntries" in json_response["resultsPage"] else 0
    amount_pages = ceil(amount_events / 50.0)
    while page <= amount_pages:
        if json_response["resultsPage"]["status"] == "ok":
            lines.extend(get_events(json_response, artistname, artistid, source))
            page += 1
            url = base_url.format(artistid, api_key, page)
            json_response = loads(get(url).text)


def check_if_artist_in_performance(artistid, event, source):
    for artist in event["performance"]:
        identifiers = artist["artist"]["identifier"]
        mscbrnz_id = identifiers[0]["mbid"] if len(identifiers) == 1 else "null"
        if "mscbrnz" in source:
            if mscbrnz_id == artistid:
                return mscbrnz_id
        else:
            if str(artist["artist"]["id"]) == str(artistid):
                return mscbrnz_id


def get_events(json_response, artistname, artistid, source):
    tmp_lines = []
    for event in json_response["resultsPage"]["results"]["event"]:
        mscbrnz_id = check_if_artist_in_performance(artistid, event, source)
        if mscbrnz_id:
            tmp_lines.append({
                "titel": event["displayName"].strip(),
                "datum": datetime.strptime(event["start"]["date"], "%Y-%m-%d"),
                "artiest": artistname,
                "mbid": mscbrnz_id,
                "stad": event["location"]["city"].split(",")[0].strip(),
                "land": event["location"]["city"].split(",")[-1].strip(),
                "venue": event["venue"]["displayName"].strip(),
                "latitude": event["venue"]["lat"],
                "longitude": event["venue"]["lng"],
                "source": source
            })
        else:
            errors.append({"source": source, "artiest": artistname, "id": artistid})
    return tmp_lines

errors = list()
groepen = read_csv("https://docs.google.com/spreadsheets/d/173K9EyJswhdSLZodGIjqFOU8wpUMYb_JOXmz2bebxDg/pub?gid=0&single=true&output=tsv", sep="\t")
bands = get_musicbrainz_belgian_artist_ids(update=False)
bands.extend(groepen["Songkick"].values)

set_useragent("kunstenpunt", "0.1", "github.com/kunstenpunt")

for band in bands:
    if "songkick.com" in str(band):
        artist_id, artist_name = band.split("/")[-1].split("-")[0], " ".join(band.split("/")[-1].split("-")[1:])
        print(artist_name)

        # past events
        past_events_url = "http://api.songkick.com/api/3.0/artists/{0}/gigography.json?apikey={1}&page={2}&min_date=2013-01-01"
        go_through_api_event_pages(past_events_url, artist_name, artist_id, "songkick gold")

        # future events
        future_events_url = "http://api.songkick.com/api/3.0/artists/{0}/calendar.json?apikey={1}"
        go_through_api_event_pages(future_events_url, artist_name, artist_id, "songkick gold")

    else:
        if len(str(band).split("\t")) > 2:
            bandnaam, bandid = str(band).split("\t")[0:2]
            print(bandnaam)

            # past events
            past_events_url = "http://api.songkick.com/api/3.0/artists/mbid:{0}/gigography.json?apikey={1}&page={2}&min_date=2013-01-01"
            go_through_api_event_pages(past_events_url, bandnaam, bandid, "songkick mscbrnz")

            # future events
            future_events_url = "http://api.songkick.com/api/3.0/artists/mbid:{0}/calendar.json?apikey={1}"
            go_through_api_event_pages(future_events_url, bandnaam, bandid, "songkick mscbrnz")

DataFrame(lines).to_excel("providers/songkick.xlsx", index=False)
if len(errors) > 0:
    DataFrame(errors).drop_duplicates(inplace=False).to_excel("songkick_errors.xlsx", index=False)
