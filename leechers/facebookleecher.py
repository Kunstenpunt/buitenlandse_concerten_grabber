from leechers.platformleecher import PlatformLeecher
import facebook
from os import remove
from dateparser import parse as dateparse
from datetime import datetime
from requests import exceptions
from pandas import Timestamp


class FacebookEventLeecher(PlatformLeecher):
    def __init__(self, root="./leechers/resources/"):
        super().__init__(root=root)
        with open(root + "facebook_access_token.txt") as f:
            fb_token = f.read()
        self.graph = facebook.GraphAPI(access_token=fb_token, version=2.10)
        self.platform = "facebook"
        try:
            remove("facebook_errors.txt")
        except FileNotFoundError:
            pass

    def set_events_for_identifier(self, band, mbid, url):
        page_label = url.split("/")[-1].split("-")[-1] if "-" in url.split("/")[-1] else url.split("/")[-1]
        print(page_label)
        try:
            events = self.graph.get_connections(id=page_label, connection_name="events")
            print(events)
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
        land = concert["place"]["location"]["country"] if "place" in concert and "location" in concert["place"] and "country" in concert["place"]["location"] else None
        if state is not None and stad is not None and land in ["United States", "Brazil", "Canada", "Australia"]:
            stad = stad + ", " + state
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
