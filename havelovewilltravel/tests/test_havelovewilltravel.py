import unittest
from havelovewilltravel.havelovewilltravel import HaveLoveWillTravel
from datetime import datetime
from json import loads


class TestHaveLoveWillTravel(unittest.TestCase):
    def setUp(self):
        self.hlwt = HaveLoveWillTravel(root="../../")

    def test_mr_henry(self):
        data = {"artiest_mb_id": "tom van kunstenpunt", "event_id": 123, "titel": "voor q", "stad_clean": "Brussel",
                "land_clean": "Belgique", "iso_code_clean": "BE", "concert_id": 123,
                "artiest_merge_naam": "tom ruette", "datum": datetime(2010, 1, 2), "source_0": "testsource0",
                "source_link_0": "sourcelink0", "source_1": "testsource1", "source_link_1": "testsourcelink1"}
        data = {"stad_clean": "Berlin", "longitude": 13.40495, "artiest_merge_naam": "Front 242", "event_id": "songkick_39064771",
                "titel": "out of line weekender", "land_clean": "Germany", "iso_code_clean": "DE", "artiest_mb_id": "Front 242",
                "source_link_0": "songkick.com/concerts/39064771", "source_0": "Songkick", "datum": "2020/05/14", "concert_id": 197658.0}
        r = self.hlwt._send_record_to_mr_henry_api(data, test=False)
        print(r.text, r.status_code)
        self.assertEqual(r.status_code, 200)
        self.maxDiff = None
        self.assertEqual(loads(r.content)["am"], data["artiest_merge_naam"])
        self.assertTrue(r.headers["X-Unit-Test"])
