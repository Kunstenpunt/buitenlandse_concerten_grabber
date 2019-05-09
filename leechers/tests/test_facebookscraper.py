import unittest
from leechers.facebookscraper import FacebookScraper
from bs4 import BeautifulSoup
from codecs import open


class TestEventLeecher(unittest.TestCase):
    def setUp(self):
        self.fbleecher = FacebookScraper(root="./")
        self.test_file = "facebook_event_test.html"

    def test_facebook_leech(self):
        self.fbleecher.set_events_for_identifier("Dimitri Vegas and Like Mike", "013a4948-dc8b-4833-9050-1084c9ae675b", "https://www.facebook.com/dimitrivegasandlikemike")
        #self.assertGreater(len(self.fbleecher.events), 0)

    def test_facebook_get_event(self):
        print(self.fbleecher._get_event("", "facebook_event_test.html", test=True))

    def test_facebook_get_date(self):
        with open(self.test_file, "r", "utf-8") as f:
            r = f.read()
        soup = BeautifulSoup(r, 'html.parser')
        print(self.fbleecher._get_datum(soup))

    def test_facebook_get_location(self):
        with open(self.test_file, "r", "utf-8") as f:
            r = f.read()
        soup = BeautifulSoup(r, 'html.parser')
        print(self.fbleecher._get_location(soup))

    def test_facebook_get_event(self):
        print(self.fbleecher._get_event("894015313944844"))
