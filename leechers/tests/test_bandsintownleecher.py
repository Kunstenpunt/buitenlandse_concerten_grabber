import unittest
from leechers.bandsintownleecher import BandsInTownLeecher


class TestEventLeecher(unittest.TestCase):
    def setUp(self):
        self.bitleecher = BandsInTownLeecher(root="../../")

    def test_bandsintown_leech(self):
        self.bitleecher.set_events_for_identifier("dEUS", "013a4948-dc8b-4833-9050-1084c9ae675b", "https://bandsintown.com/deus")
        self.assertGreater(len(self.bitleecher.events), 0)

    def test_bandsintown_broken_link(self):
        self.bitleecher.set_events_for_identifier("Milk Inc.", "013a4948-dc8b-4833-9050-1084c9ae675b", "https://www.bandsintown.com/azertyuiop")
        self.assertListEqual(self.bitleecher.events, [])
