import unittest
from leechers.setlistleecher import SetlistFmLeecher


class TestEventLeecher(unittest.TestCase):
    def setUp(self):
        self.setlistleecher = SetlistFmLeecher(root="../../")

    def test_setlist_leech(self):
        self.setlistleecher.set_events_for_identifier("The Guru Guru", "013a4948-dc8b-4833-9050-1084c9ae675b", "https://www.setlist.fm/setlists/steak-number-eight-bd22d12.html")
        self.assertGreater(len(self.setlistleecher.events), 0)

    def test_setlist_broken_link(self):
        self.setlistleecher.set_events_for_identifier("Milk Inc", "nononono", "https://www.setlist.fm/setlists/azertyuiop.html")
        self.assertListEqual(self.setlistleecher.events, [])
