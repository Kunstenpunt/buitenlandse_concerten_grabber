import unittest
from havelovewilltravel.havelovewilltravel import HaveLoveWillTravel
from datetime import datetime
from json import loads
from pandas import read_excel


class TestHaveLoveWillTravel(unittest.TestCase):
    def setUp(self):
        self.hlwt = HaveLoveWillTravel(root="../../")

    def test_mr_henry(self):
        data = {"artiest_mb_id": "tom van kunstenpunt", "event_id": 123, "titel": "voor q", "stad_clean": "Brussel",
                "land_clean": "Belgique", "iso_code_clean": "BE", "concert_id": 123,
                "artiest_merge_naam": "tom ruette", "datum": datetime(2010, 1, 2), "source_0": "testsource0",
                "source_link_0": "sourcelink0", "source_1": "testsource1", "source_link_1": "testsourcelink1"}
        r = self.hlwt._send_record_to_mr_henry_api(data, test=False)
        print(r.text, r.status_code)
        self.assertEqual(r.status_code, 200)
        self.maxDiff = None
        self.assertTrue(r.headers["X-Unit-Test"])

    def test_send_everything(self):
        self.hlwt.df = read_excel("/home/tom/PycharmProjects/buitenlandse_concerten_grabber/output/latest.xlsx")
        self.hlwt.diff_event_ids = self.hlwt.df[self.hlwt.df["datum"] >= datetime(2019, 1, 1)]["event_id"].to_list()
        self.hlwt.send_data_to_mr_henry()
