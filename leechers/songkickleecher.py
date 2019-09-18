from leechers.platformleecher import PlatformLeecher
from json import loads, decoder
from requests import get
from math import ceil
from pandas import Timestamp, DataFrame
from dateparser import parse as dateparse
from datetime import datetime


class SongkickLeecher(PlatformLeecher):
    def __init__(self, root="./leechers/resources/"):
        super().__init__(root=root)
        with open(root + "songkick_api_key.txt") as f:
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
            resultspage = json_response["resultsPage"]
            amount_events = resultspage["totalEntries"] if "totalEntries" in resultspage else 0
            amount_pages = ceil(amount_events / 50.0)
            while page <= amount_pages:
                if resultspage["status"] == "ok":
                    for event in resultspage["results"]["event"]:
                        self.events.append(self.map_platform_to_schema(event, band, mbid, {"artist_id": artistid, "artist_name": artistname}))
                    page += 1
                    url = base_url.format(artistid, self.platform_access_granter, page)
                    html = get(url).text
                    try:
                        resultspage = loads(html)["resultsPage"]
                    except decoder.JSONDecodeError:
                        print("decoder error")
                        resultspage = {"status": "nok"}

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


if __name__ == "__main__":
    leecher = SongkickLeecher(root="../")
    leecher.set_platform_identifiers()
    leecher.set_events_for_identifiers()
    current = DataFrame(leecher.events)
    current.to_excel("output/" + datetime.now().date().isoformat() + "_songkick.xlsx")
