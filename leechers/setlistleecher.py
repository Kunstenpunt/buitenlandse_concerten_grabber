from json import loads, decoder
from requests import get
from dateparser import parse as dateparse
from pandas import Timestamp, DataFrame
from leechers.platformleecher import PlatformLeecher
from datetime import datetime


class SetlistFmLeecher(PlatformLeecher):
    def __init__(self, root="./leechers/resources/"):
        super().__init__(root=root)
        with open(root + "setlist_api_key.txt", "r") as f:
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
        if state is not None and concert["venue"]["city"]["country"]["code"] in ["US", "Brazil", "Australia", "Canada"]:
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


if __name__ == "__main__":
    leecher = SetlistFmLeecher(root="../")
    leecher.set_platform_identifiers()
    leecher.set_events_for_identifiers()
    current = DataFrame(leecher.events)
    current.to_excel("output/" + datetime.now().date().isoformat() + "_setlistfm.xlsx")
