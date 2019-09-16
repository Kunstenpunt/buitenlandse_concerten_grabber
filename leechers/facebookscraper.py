from leechers.platformleecher import PlatformLeecher
from urllib import parse as urlparse
from json import decoder, loads, dumps
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
            try:
                r = get(url, headers=headers)
            except Exception as e:
                sleep(360)
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
                try:
                    r = get(url, headers=headers).text
                except Exception as e:
                    sleep(360)
                    r = get(url, headers=headers).text
        else:
            with open(test_file, "r", "utf-8") as f:
                r = f.read()
        soup = BeautifulSoup(r, 'html.parser')
        try:
            ld = loads(soup.find("script", {"type": "application/ld+json"}).text)
            print(ld)
            datum = self._get_datum(ld)
            location = self._get_location(ld)
            titel = self._get_title(ld)
            event_data = {
                "event_id": event_id,
                "datum": datum,
                "land": location["country"],
                "stad": location["city"],
                "venue": location["venue"],
                "latitude": location["lat"],
                "longitude": location["lng"],
                "titel": titel
            }
            print(event_data)
        except AttributeError:
            event_data = {}
        return event_data

    @staticmethod
    def _get_title(ld):
        return ld["name"]

    @staticmethod
    def _get_datum(ld):
        return dateparse(ld["startDate"]).date()

    def _get_location(self, ld):
        try:
            location_name = ld["location"]["name"]
        except KeyError:
            location_name = ""
        try:
            location_street = ld["location"]["address"]["addressLocality"]
        except KeyError:
            location_street = ""
        try:
            location_country = ld["location"]["address"]["addressCountry"]
        except KeyError:
            location_country = ""
        loc_info = self.get_lat_lon_for_venue(location_name, location_street, location_country)
        loc_info["venue"] = location_name
        loc_info["city"] = location_street,
        loc_info["country"] = location_country,
        return loc_info

    def set_events_for_identifier(self, band, mbid, url):
        print(band, mbid, url)

        urls = ["http://mobile.facebook" + "facebook".join(url.split("facebook")[1:]) + "/events"]

        for url in urls:
            event_ids = self._get_event_ids(url)

            events = []
            for event_id in event_ids:
                print("grabbing raw data for", event_id)
                concert = self._get_event(event_id)
                events.append(concert)

            if events:
                for concert in events:
                    if isinstance(concert, dict) and "event_id" in concert:
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
