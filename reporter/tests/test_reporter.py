import unittest
from reporter.reporter import Reporter


class TestReporter(unittest.TestCase):
    def setUp(self):
        self.reporter = Reporter()

    def test_city_cleaning(self):
        self.reporter.take_snapshot_of_status("old")
        self.reporter.take_snapshot_of_status("current")
        self.reporter.compare_city_cleaning()
        self.assertTrue(isinstance(self.reporter.aantal_ongecleande_steden, int))

    def test_country_cleaning(self):
        self.reporter.take_snapshot_of_status("old")
        self.reporter.take_snapshot_of_status("current")
        self.reporter.compare_country_cleaning()
        self.assertTrue(isinstance(self.reporter.aantal_ongecleande_landen, int))

    def test_genre_toekenning(self):
        self.reporter.take_snapshot_of_status("old")
        self.reporter.take_snapshot_of_status("current")
        self.reporter.set_aantal_musicbrainz_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre()
        self.assertTrue(isinstance(self.reporter.aantal_musicbrainz_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre, int))

