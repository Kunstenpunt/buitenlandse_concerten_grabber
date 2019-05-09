from pandas import read_excel, isnull
from requests import get
from json import loads
from random import sample


class PlatformLeecher(object):
    def __init__(self, root="./leechers/resources/"):
        self.root = root
        self.platform_identifiers = []
        self.platform_access_granter = None
        self.events = []
        self.platform = None
        with open(self.root + "google_api_places_api_key.txt", "r") as f:
            self.google_places_api_key = f.read().strip()

    def set_platform_identifiers(self):
        lst = read_excel(self.root + "../../resources/" + "belgian_mscbrnz_artists.xlsx")
        ignore_list = read_excel(self.root + "../../resources/" + "ignore_list.xlsx")
        bands_done = set()
        for i in lst.index:
            row = lst.loc[i]
            if row["mbid"] not in ignore_list["mbid"].values:
                if not isnull(row[self.platform]) and row["band"] not in bands_done:
                    self.platform_identifiers.append((row[["band", "mbid", self.platform]]))
                else:
                    bands_done.add(row["band"])
            else:
                print("ignoring", row["band"])

    def set_events_for_identifiers(self, sample_size=False):
        pis = self.platform_identifiers
        if sample_size:
            pis = sample(self.platform_identifiers, sample_size)
        for band, mbid, urls in pis:
            for url in urls.split(","):
                print(self.platform, band, url)
                self.set_events_for_identifier(band, mbid, url)

    def set_events_for_identifier(self, band, mbid, url):
        raise NotImplementedError

    def map_platform_to_schema(self, event, band, mbid, other):
        raise NotImplementedError

    def get_lat_lon_for_venue(self, venue, city, country):
        url = "https://maps.googleapis.com/maps/api/place/textsearch/json?query={0}&key={1}"
        venue_search = " ".join([venue, city, country])
        city_search = " ".join([city, country])
        result = loads(get(url.format(venue_search, self.google_places_api_key)).text)
        if result["status"] == "ZERO_RESULTS":
            result = loads(get(url.format(city_search, self.google_places_api_key)).text)
        if result["status"] != "ZERO_RESULTS":
            if "types" in result["results"][0]:
                if "locality" in result["results"][0]["types"] or "postal_code" in result["results"][0]["types"]:
                    city = result["results"][0]["name"]
                    country = result["results"][0]["formatted_address"].split(", ")[-1]
            else:
                city = ", ".join(result["results"][0]["formatted_address"].split(", ")[1:-1])
                country = result["results"][0]["formatted_address"].split(", ")[-1]
            return {
                "lat": result["results"][0]["geometry"]["location"]["lat"],
                "lng": result["results"][0]["geometry"]["location"]["lng"],
                "city": city,
                "country": country
            }
        else:
            return {"lat": None, "lng": None, "city": None, "country": None}
