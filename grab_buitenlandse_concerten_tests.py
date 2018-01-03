import unittest
from grab_buitenlandse_concerten import Grabber
from pandas import DataFrame, isnull, Timestamp, Series, read_excel
from datetime import datetime, timedelta
from json import loads


class TestReporter(unittest.TestCase):
    def setUp(self):
        self.grabber = Grabber()

    def test_city_cleaning(self):
        self.grabber.reporter.take_snapshot_of_status("old")
        self.grabber.reporter.take_snapshot_of_status("current")
        self.grabber.reporter.compare_city_cleaning()
        self.assertTrue(isinstance(self.grabber.reporter.aantal_ongecleande_steden, int))

    def test_country_cleaning(self):
        self.grabber.reporter.take_snapshot_of_status("old")
        self.grabber.reporter.take_snapshot_of_status("current")
        self.grabber.reporter.compare_country_cleaning()
        self.assertTrue(isinstance(self.grabber.reporter.aantal_ongecleande_landen, int))

    def test_genre_toekenning(self):
        self.grabber.reporter.take_snapshot_of_status("old")
        self.grabber.reporter.take_snapshot_of_status("current")
        self.grabber.reporter.set_aantal_musicbrainz_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre()
        self.assertTrue(isinstance(self.grabber.reporter.aantal_musicbrainz_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre, int))

    def test_mr_henry(self):
        data = {"artiest_mb_id": "tom van kunstenpunt", "event_id": 123, "titel": "voor q",
                "artiest_merge_naam": "tom ruette", "datum": datetime(2010, 1, 2), "source_0": "testsource0",
                "source_link_0": "sourcelink0", "source_1": "testsource1", "source_link_1": "testsourcelink1"}
        r = self.grabber._send_record_to_mr_henry_api(data, test=True)
        self.assertEqual(r.status_code, 200)
        self.maxDiff = None
        self.assertEqual(loads(r.content)["artist_mb_id"], data["artiest_mb_id"])
        self.assertTrue(r.headers["X-Unit-Test"])

    def tests_do_mr_henry(self):
        self.grabber.df = read_excel("output/latest.xlsx")
        self.grabber.send_data_to_mr_henry(test=False)


class TestGrabber(unittest.TestCase):
    def setUp(self):
        self.grabber = Grabber()

    def test_lat_lon_in_belgium(self):
        antwerpen = {"latitude": 51.2603015, "longitude": 4.2176391}
        berlijn = {"latitude": 52.5067614, "longitude": 13.284651}
        self.assertTrue(self.grabber._concert_is_in_belgium(antwerpen))
        self.assertFalse(self.grabber._concert_is_in_belgium(berlijn))
    def test_convert_iso_code_to_full_name(self):
        self.assertEqual(self.grabber._convert_cleaned_country_name_to_full_name("DE"), "Germany")
        self.assertEqual(self.grabber._convert_cleaned_country_name_to_full_name("GB"), "United Kingdom")
        self.assertEqual(self.grabber._convert_cleaned_country_name_to_full_name("VV"), "Unknown")

    def test_add_podiumfestivalinfo(self):
        self.grabber.current = DataFrame([{}])
        self.grabber.mbab.load_list()
        self.grabber.add_podiumfestivalinfo_concerts()
        self.assertTrue(self.grabber.current[(self.grabber.current["artiest_mb_naam"] == "Balthazar") & (self.grabber.current["datum"] < datetime(2005, 1, 1))].empty)

    def test_infer_cancellations(self):
        self.grabber.df = DataFrame([
            {
                "event_id": "sk1",
                "datum": datetime.now() + timedelta(days=6),
                "last_seen_on": datetime.now() - timedelta(days=6)
            }
        ])
        self.grabber.infer_cancellations()
        self.assertTrue(self.grabber.df[self.grabber.df["event_id"] == "sk1"]["cancelled"].bool())

    def test_clean_names(self):
        self.grabber.df = DataFrame([
            {
                "venue": "AB"
            }
        ])
        self.grabber._clean_names("venue", "venue_clean", "resources/venue_cleaning.xlsx")
        self.assertEqual(self.grabber.df.iloc[0]["venue_clean"], "Ancienne Belgique")

    def test_prefer_precise_date_for_festival(self):
        self.grabber.df = DataFrame([
            {
                "event_id": "sk1",
                "event_type": "Festival",
                "datum": Timestamp(datetime.now().date() - timedelta(days=2)),
                "einddatum": Timestamp(datetime.now().date() + timedelta(days=3)),
                "last_seen_on": datetime.now().date(),
                "source": "songkick",
                "artiest_mb_id": "a",
                "stad_clean": "b",
                "visible": True,
                "concert_id": 1
            },
            {
                "event_id": "bit1",
                "datum": Timestamp(datetime.now().date()),
                "last_seen_on": datetime.now().date(),
                "source": "bandsintown",
                "artiest_mb_id": "a",
                "stad_clean": "b",
                "visible": False,
                "concert_id": 2
            },
            {
                "event_id": "setlist1",
                "datum": Timestamp(datetime.now().date()),
                "last_seen_on": datetime.now().date(),
                "source": "setlist",
                "artiest_mb_id": "a",
                "stad_clean": "b",
                "visible": False,
                "concert_id": 2
            },
            {
                "event_id": "setlist1",
                "datum": None,
                "source": "setlist",
                "last_seen_on": datetime.now().date(),
                "artiest_mb_id": "a",
                "stad_clean": "b",
                "visible": False,
                "concert_id": 2
            }
        ])
        self.grabber._set_precise_date_for_festivals()
        self.assertEqual(self.grabber.df.loc[0]["visible"], False)
        self.assertEqual(self.grabber.df.loc[0]["concert_id"], 2)
        self.assertEqual(self.grabber.df.loc[1]["visible"], True)

        self.grabber.df = read_excel("output/latest.xlsx")
        self.grabber._set_precise_date_for_festivals()

    def test_update_field_based_on_new_leech(self):
        self.grabber.df = DataFrame([
            {
                "event_id": "sk1",
                "titel": "test1",
                "artiest_mb_id": "a",
                "last_seen_on": datetime.now().date() - timedelta(days=2)
            },
            {
                "event_id": "sk1",
                "titel": "test2",
                "artiest_mb_id": "a",
                "last_seen_on": datetime.now().date()
            }
        ])
        self.grabber._update_field_based_on_new_leech("titel")
        self.assertEqual(self.grabber.df["titel"].values[0], "test2")

    def test_update_last_seen_on_field(self):
        self.grabber.previous = DataFrame([
            {
                "event_id": "sk1",
                "titel": "test1",
                "artiest_mb_id": "a",
                "last_seen_on": datetime.now().date() - timedelta(days=2)
            },
            {
                "event_id": "sk2",
                "titel": "test2",
                "artiest_mb_id": "a",
                "last_seen_on": datetime.now().date() - timedelta(days=2)
            },
            {
                "event_id": "sk3",
                "titel": "test3",
                "artiest_mb_id": "a",
                "last_seen_on": datetime.now().date() - timedelta(days=2)
            }
        ])
        self.grabber.current = DataFrame([
            {
                "event_id": "sk2",
                "titel": None,
                "artiest_mb_id": "a",
                "last_seen_on": datetime.now().date()
            },
            {
                "event_id": "sk3",
                "titel": "test3",
                "artiest_mb_id": "a",
                "last_seen_on": datetime.now().date()
            },
            {
                "event_id": "sk4",
                "titel": "test4",
                "artiest_mb_id": "a",
                "last_seen_on": datetime.now().date()
            }
        ])
        self.grabber.df = self.grabber.previous.append(self.grabber.current, ignore_index=True)
        self.grabber.df.drop_duplicates(subset=["artiest_mb_id", "event_id"], keep="first", inplace=True)
        self.grabber._update_last_seen_on_dates_of_previous_events_that_are_still_current()
        self.assertEqual(self.grabber.df[self.grabber.df["event_id"] == "sk2"]["last_seen_on"].values[0], datetime.now().date())

    def test_create_diff(self):
        self.grabber.previous = DataFrame([
            {
                "event_id": "sk1",
                "titel": "test1",
                "artiest_mb_id": "a",
                "last_seen_on": datetime.now().date() - timedelta(days=2)
            },
            {
                "event_id": "sk2",
                "titel": "test2",
                "artiest_mb_id": "a",
                "last_seen_on": datetime.now().date() - timedelta(days=2)
            },
            {
                "event_id": "sk3",
                "titel": "test3",
                "artiest_mb_id": "a",
                "last_seen_on": datetime.now().date() - timedelta(days=2)
            }
        ])
        self.grabber.current = DataFrame([
            {
                "event_id": "sk2",
                "titel": None,
                "artiest_mb_id": "a",
                "last_seen_on": datetime.now().date()
            },
            {
                "event_id": "sk3",
                "titel": "test3",
                "artiest_mb_id": "a",
                "last_seen_on": datetime.now().date()
            },
            {
                "event_id": "sk3",
                "titel": "test3",
                "artiest_mb_id": "b",
                "last_seen_on": datetime.now().date()
            },
            {
                "event_id": "sk4",
                "titel": "test4",
                "artiest_mb_id": "a",
                "last_seen_on": datetime.now().date()
            }
        ])
        self.grabber.df = self.grabber.previous.append(self.grabber.current, ignore_index=True)
        self.grabber.df.drop_duplicates(subset=["artiest_mb_id", "event_id"], keep="first", inplace=True)
        self.grabber._generate_diff()
        self.assertEqual(self.grabber.diff.index.tolist(), [2, 3])

    def test_match_artiest_naam_to_mbid(self):
        self.grabber.mbab.load_list()
        self.assertIsNotNone(self.grabber._match_artist_name_to_mbid("Pornorama"))
        self.assertIsNone(self.grabber._match_artist_name_to_mbid("Onbekende artiest"))

    def test_make_gig_triples(self):
        self.grabber.df = DataFrame([
            {
                "event_id": "sk1",
                "datum": datetime.now().date(),
                "artiest_merge_naam": "a",
                "stad_clean": "b"
            },
            {
                "event_id": "sk2",
                "datum": datetime.now().date(),
                "artiest_merge_naam": "a",
                "stad_clean": "b"
            }
        ])

        gig_triples = self.grabber._make_gig_triples()
        self.assertEqual(gig_triples, {('a', datetime.now().date(), 'b')})

    def test_assign_concert_ids(self):
        self.grabber.df = DataFrame([
            {
                "event_id": "sk1",
                "datum": datetime.now().date(),
                "artiest_merge_naam": "a",
                "stad_clean": "b"
            },
            {
                "event_id": "sk2",
                "datum": datetime.now().date(),
                "artiest_merge_naam": "a",
                "stad_clean": "b"
            }
        ])
        gig_triples = {('a', datetime.now().date(), 'b')}
        self.grabber._assign_concert_ids(gig_triples)
        self.assertListEqual(self.grabber.df["concert_id"].tolist(), [0, 0])

    def test_select_visibility(self):
        self.grabber.now = datetime.now()
        self.grabber.df = DataFrame([
            {
                "event_id": "bit1",
                "source": "bandsintown",
                "datum": datetime.now().date() + timedelta(days=2),
                "last_seen_on": datetime.now().date() - timedelta(days=1),
                "artiest_merge_naam": "a",
                "stad_clean": "b",
                "concert_id": 0,
                "ignore": False,
                "latitude": 51.2603015,
                "longitude": 13.2176391
            },
            {
                "event_id": "sk1",
                "source": "songkick",
                "datum": datetime.now().date() + timedelta(days=2),
                "last_seen_on": datetime.now().date() - timedelta(days=7),
                "artiest_merge_naam": "a",
                "stad_clean": "b",
                "concert_id": 0,
                "ignore": False,
                "latitude": 51.2603015,
                "longitude": 13.2176391
            }
        ])
        self.grabber._select_visibility_per_concert()
        self.assertTrue(self.grabber.df.iloc[0]["visible"])
        self.assertFalse(self.grabber.df.iloc[1]["visible"])

        self.grabber.now = datetime.now()
        self.grabber.df = DataFrame([
            {
                "event_id": "bit1",
                "source": "bandsintown",
                "datum": datetime.now().date() + timedelta(days=2),
                "last_seen_on": datetime.now().date() - timedelta(days=1),
                "artiest_merge_naam": "a",
                "stad_clean": "b",
                "concert_id": 0,
                "ignore": False,
                "latitude": 51.2603015,
                "longitude": 13.2176391
            }
        ])
        self.grabber._select_visibility_per_concert()
        self.assertTrue(self.grabber.df.iloc[0]["visible"])

        self.grabber.df = read_excel("output/latest.xlsx")
        self.grabber._select_visibility_per_concert()

    def test_fix_weird_symbols(self):
        self.assertIsInstance(self.grabber._fix_weird_symbols("ü@sdf£µù)°ñ"), str)

    def test_handle_ambiguous_names(self):
        self.grabber.df = DataFrame([
            {
                "artiest_merge_naam": None,
                "artiest_mb_naam": "Guy Verlinde"
            }
        ])
        self.grabber.handle_ambiguous_artists()
        self.assertIsNotNone(self.grabber.df.loc[0, "artiest_merge_naam"])

    def test_infer_cancellations(self):
        self.grabber.df = DataFrame([
            {
                "datum": Timestamp(datetime.now().date() + timedelta(days=1)),
                "last_seen_on": Timestamp(datetime.now().date() - timedelta(days=6))
            },
            {
                "datum": Timestamp(datetime.now().date() + timedelta(days=1)),
                "last_seen_on": Timestamp(datetime.now().date() - timedelta(days=2))
            }
        ])
        row = self.grabber.df.iloc[0]
        self.assertTrue(self.grabber._is_cancellation(row))
        self.grabber.infer_cancellations()
        self.assertListEqual(self.grabber.df["cancelled"].tolist(), [True, False])

    def test_set_source_link(self):
        self.grabber.df = DataFrame([
            {
                "event_id": "facebook884162221625243",
                "concert_id": 1,
                "visible": True
            },
            {
                "event_id": "songkick_5318473",
                "concert_id": 2,
                "visible": True
            },
            {
                "event_id": "facebook_5318473",
                "concert_id": 2,
                "visible": False
            },
            {
                "event_id": "beren_gieren_1",
                "concert_id": 3,
                "visible": True
            }
        ])
        self.grabber._set_source_outlinks_per_concert()
        print(self.grabber.df)

    def test_establish_source_hyperlink(self):
        self.assertEqual(self.grabber._establish_source_hyperlink("facebook9871237"), ("Facebook", "9871237"))

if __name__ == '__main__':
    unittest.main()
