import unittest
from grab_buitenlandse_concerten import Grabber
from pandas import DataFrame, isnull, Timestamp, read_excel
from datetime import datetime, timedelta


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


class TestGrabber(unittest.TestCase):
    def setUp(self):
        self.grabber = Grabber()

    def test_convert_iso_code_to_full_name(self):
        self.assertEqual(self.grabber._convert_cleaned_country_name_to_full_name("DE"), "Germany")
        self.assertEqual(self.grabber._convert_cleaned_country_name_to_full_name("VV"), "Unknown")

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
        # TODO
        pass

    def test_prefer_precise_date_for_festival(self):
        self.grabber.df = DataFrame([
            {
                "event_id": "sk1",
                "event_type": "Festival",
                "datum": Timestamp(datetime.now().date() - timedelta(days=2)),
                "einddatum": Timestamp(datetime.now().date() + timedelta(days=3)),
                "source": "songkick",
                "artiest_mb_id": "a",
                "stad_clean": "b",
                "visible": True,
                "concert_id": 1
            },
            {
                "event_id": "bit1",
                "datum": Timestamp(datetime.now().date()),
                "source": "bandsintown",
                "artiest_mb_id": "a",
                "stad_clean": "b",
                "visible": False,
                "concert_id": 2
            },
            {
                "event_id": "setlist1",
                "datum": Timestamp(datetime.now().date()),
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
        self.assertEqual(gig_triples, set([('a', datetime.now().date(), 'b')]))

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
        gig_triples = set([('a', datetime.now().date(), 'b')])
        self.grabber._assign_concert_ids(gig_triples)
        self.assertListEqual(self.grabber.df["concert_id"].tolist(), [0, 0])

    def test_select_visibility(self):
        self.grabber.df = DataFrame([
            {
                "event_id": "bit1",
                "source": "bandsintown",
                "datum": datetime.now().date(),
                "artiest_merge_naam": "a",
                "stad_clean": "b",
                "concert_id": 0
            },
            {
                "event_id": "sk1",
                "source": "songkick",
                "datum": datetime.now().date(),
                "artiest_merge_naam": "a",
                "stad_clean": "b",
                "concert_id": 0
            }
        ])
        self.grabber._select_visibility_per_concert()
        self.assertTrue(self.grabber.df.iloc[1]["visible"])
        self.assertTrue(isnull(self.grabber.df.iloc[0]["visible"]))

    def test_fix_weird_symbols(self):
        self.assertIsInstance(self.grabber._fix_weird_symbols("ü@sdf£µù)°ñ"), str)


if __name__ == '__main__':
    unittest.main()
