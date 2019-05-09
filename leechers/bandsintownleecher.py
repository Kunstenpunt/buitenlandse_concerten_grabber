import bandsintown
from time import sleep
from leechers.platformleecher import PlatformLeecher
from urllib import parse as urlparse
from json import decoder
from pandas import Timestamp, DataFrame
from dateparser import parse as dateparse
from datetime import datetime


class BandsInTownLeecher(PlatformLeecher):
    def __init__(self, root="./leechers/resources/"):
        super().__init__(root=root)
        self.bitc = bandsintown.Client("kunstenpunt")
        self.platform = "bandsintown"

    def set_events_for_identifier(self, band, mbid, url):
        period = "1900-01-01,2050-01-01"
        bandnaam = urlparse.unquote(url.split("/")[-1].split("?came_from")[0])
        events = None
        trials = 0
        while events is None and trials < 10:
            trials += 1
            try:
                events = self.bitc.artists_events(bandnaam, date=period)
                if events is not None:
                    while "errors" in events:
                        print(events["errors"])
                        if "Rate limit exceeded" in events["errors"]:
                            print("one moment!")
                            sleep(60.0)
                            events = self.bitc.artists_events(bandnaam, date=period)
                        else:
                            events = []
            except decoder.JSONDecodeError:
                events = None

        if events:
            for concert in events:
                if isinstance(concert, dict):
                    self.events.append(self.map_platform_to_schema(concert, band, mbid, {}))

    def map_platform_to_schema(self, concert, band, mbid, other):
        region = concert["venue"]["region"].strip() if "region" in concert["venue"] else None
        stad = (concert["venue"]["city"]).strip()
        if region is not None and (concert["venue"]["country"]).strip() in ["Ac United States", "United States", "Canada", "Brazil", "Australia"]:
            stad = stad + ", " + region.strip()
        return {
            "datum": Timestamp(dateparse(concert["datetime"]).date()),
            "land": (concert["venue"]["country"]).strip(),
            "stad": stad,
            "venue": (concert["venue"]["name"]).strip() if "name" in concert["venue"] else None,
            "titel": (concert["description"]).strip(),
            "titel_generated": band + "@" + (concert["venue"]["name"] if "name" in concert["venue"] else "Unknown").strip(),# (concert["description"]).strip(),
            "artiest": band, #self.__get_artist_naam(concert),
            "artiest_mb_naam": band,
            "artiest_id": "bandsintown_" + str(concert["artist_id"]),
            "artiest_mb_id": mbid,
            "event_id": "bandsintown_" + str(concert["id"]),
            "latitude": concert["venue"]["latitude"] if "latitude" in concert["venue"] else None,
            "longitude": concert["venue"]["longitude"] if "longitude" in concert["venue"] else None,
            "source": self.platform
        }

    @staticmethod
    def __get_artist_naam(concert):
        for artist in concert["artists"]:
            if artist["id"] == concert["artist_id"]:
                return artist["name"]


if __name__ == "__main__":
    leecher = BandsInTownLeecher(root="../")
    leecher.set_platform_identifiers()
    leecher.set_events_for_identifiers()
    current = DataFrame(leecher.events)
    current.to_excel("output/" + datetime.now().date().isoformat() + "_bandsintown.xlsx")
