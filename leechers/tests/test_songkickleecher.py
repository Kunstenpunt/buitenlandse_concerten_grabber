import unittest
from leechers.songkickleecher import SongkickLeecher


class TestEventLeecher(unittest.TestCase):
    def setUp(self):
        self.skleecher = SongkickLeecher(root="../../")

    def test_songkick_leech(self):
        self.skleecher.set_events_for_identifier("Front 242", "013a4948-dc8b-4833-9050-1084c9ae675b", "https://www.songkick.com/artists/15293-front-242")
        self.assertGreater(len(self.skleecher.events), 0)

    def test_songkick_broken_link(self):
        self.skleecher.set_events_for_identifier("Milk Inc.", "nonononon", "https://www.songkick.com/artists/azertyuiop")
        self.assertListEqual(self.skleecher.events, [])
