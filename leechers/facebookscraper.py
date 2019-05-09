from leechers.platformleecher import PlatformLeecher
from urllib import parse as urlparse
from json import decoder
from pandas import Timestamp, DataFrame
from dateparser import parse as dateparse
from datetime import datetime
from time import sleep
from requests import get
from fake_useragent import UserAgent
from re import compile
from codecs import open
from bs4 import BeautifulSoup


class FacebookScraper(PlatformLeecher):
    def __init__(self, root="./leechers/resources/"):
        super().__init__(root=root)
        self.platform = "facebook"
        self.ua = UserAgent()

    def _get_event_ids(self, url):
        print(url)
        regex = compile('href="/events/(\d+?)\?')
        headers = {'user-agent': self.ua.random}
        sleep(10.0)
        try:
            r = get(url, headers=headers)
        except Exception as e:
            sleep(60.0)
            r = get(url, headers=headers)
        event_ids = regex.findall(r.text)
        return event_ids

    def _get_event(self, event_id, test_file=None, test=False):
        url = "http://mobile.facebook.com/events/" + event_id
        headers = {'user-agent': self.ua.random}
        if not test:
            sleep(10.0)
            try:
                r = get(url, headers=headers).text
            except Exception as e:
                sleep(60.0)
                r = get(url, headers=headers).text
        else:
            with open(test_file, "r", "utf-8") as f:
                r = f.read()
        soup = BeautifulSoup(r, 'html.parser')
        print(soup)
        datum = self._get_datum(soup)
        location = self._get_location(soup)
        titel = self._get_title(soup)
        return {
            "event_id": event_id,
            "datum": datum,
            "land": location["country"],
            "stad": location["city"],
            "venue": location["venue"],
            "latitude": location["lat"],
            "longitude": location["lng"],
            "titel": titel
        }

    @staticmethod
    def _get_title(soup):
        class_title = soup.find("h3", {"class": "_31y8"})
        if class_title:
            return class_title.get_text(" ")
        else:
            return soup.find("title")

    @staticmethod
    def _get_datum(soup):
        event_deixis_class = "_56hq _4g33 _533c"
        deixis = soup.find_all("div", {"class": event_deixis_class})
        for div in deixis:
            title = div["title"]
            date = (dateparse(" ".join(title.split(" ")[0:4])))
            if date is not None:
                return date

    def _get_location(self, soup):
        event_deixis_class = ["_56hq _4g33 _533c", "cn co"]
        event_location_id = ["u_0_a", "u_0_1", "u_0_9"]
        deixis = soup.find_all("div", {"class": event_deixis_class, "id": event_location_id})
        loc_info = {"city": None, "country": None, "lat": None, "lng": None, "venue": None}
        if len(deixis) > 0:
            div = deixis[0]
            venue_search_string = div.get_text(" || ")
            loc_info = self.get_lat_lon_for_venue(venue_search_string.replace(" || ", " "), " ".join(venue_search_string.split(" || ")[1:]), "")
            loc_info["venue"] = div["title"]
        return loc_info

    def set_events_for_identifier(self, band, mbid, url):
        print(band, mbid, url)

        url = "http://mobile.facebook" + "facebook".join(url.split("facebook")[1:]) + "/events?is_past=1"

        event_ids = self._get_event_ids(url)

        events = []
        for event_id in event_ids:
            print("grabbing raw data for", event_id)
            concert = self._get_event(event_id)
            events.append(concert)

        if events:
            for concert in events:
                if isinstance(concert, dict):
                    print("cleaning up data for", concert["event_id"])
                    self.events.append(self.map_platform_to_schema(concert, band, mbid, {"url": url}))

        print("we now have", len(self.events), "concerts grabbed from facebook")

    def map_platform_to_schema(self, concert, band, mbid, other):
        return {
            "datum": concert["datum"],
            "land": concert["land"],
            "stad": concert["stad"],
            "venue": concert["venue"],
            "titel": concert["titel"],
            "titel_generated": str(band) + " @ " + str(concert["venue"]) + ", " + str(concert["stad"]),
            "artiest": band,
            "artiest_mb_naam": band,
            "artiest_id": "facebook_" + other["url"].strip("/").split("/")[-2],
            "artiest_mb_id": mbid,
            "event_id": "facebook_" + concert["event_id"],
            "latitude": concert["latitude"],
            "longitude": concert["longitude"],
            "source": self.platform
        }


if __name__ == "__main__":
    leecher = FacebookScraper(root="./")
    leecher.set_platform_identifiers()
    leecher.set_events_for_identifiers(sample_size=500)
    current = DataFrame(leecher.events)
    current.to_excel("output/" + datetime.now().date().isoformat() + "facebookscraper.xlsx")
