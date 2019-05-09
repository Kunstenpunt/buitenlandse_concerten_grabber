import unittest
from leechers.facebookleecher import FacebookEventLeecher


class TestEventLeecher(unittest.TestCase):
    def setUp(self):
        self.fbleecher = FacebookEventLeecher(root="../../")

    def test_facebook_leech(self):
        self.fbleecher.set_events_for_identifier("Dimitri Vegas and Like Mike", "013a4948-dc8b-4833-9050-1084c9ae675b", "https://www.facebook.com/dimitrivegasandlikemike")
        #self.assertGreater(len(self.fbleecher.events), 0)
