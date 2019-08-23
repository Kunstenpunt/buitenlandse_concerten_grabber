from json import load, dump
from pandas import read_excel
from codecs import open


class Reporter(object):
    def __init__(self, now, drive):
        with open("./reporter/output/report.json", "r", "utf-8") as f:
            self.previous_report = load(f)

        self.drive = drive
        self.aantal_musicbrainz_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre = None
        self.old_status_ignore = None
        self.old_status_musicbrainz = None
        self.old_status_city_cleaning = None
        self.old_status_country_cleaning = None
        self.current_status_ignore = None
        self.current_status_city_cleaning = None
        self.current_status_country_cleaning = None
        self.current_status_musicbrainz = None
        self.aantal_ongecleande_steden = None
        self.aantal_ongecleande_landen = None
        self.report = None
        self.datum_vorige_check = None
        self.datum_recentste_check = now

    def set_aantal_musicbrainz_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre(self):
        has_concerts = self.current_status_musicbrainz["number_of_concerts"] > 0
        genre_is_rest = self.current_status_musicbrainz["maingenre"].isnull()
        aantal = len(self.current_status_musicbrainz[has_concerts & genre_is_rest].index)
        self.aantal_musicbrainz_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre = aantal

    def take_snapshot_of_status(self, timing):
        if timing == "old":
            self.old_status_ignore = read_excel("resources/ignore_list.xlsx")
            self.old_status_musicbrainz = read_excel("resources/belgian_mscbrnz_artists.xlsx")
            self.old_status_city_cleaning = read_excel("resources/city_cleaning.xlsx")
            self.old_status_country_cleaning = read_excel("resources/country_cleaning.xlsx")
        if timing == "current":
            self.current_status_ignore = read_excel("resources/ignore_list.xlsx")
            self.current_status_city_cleaning = read_excel("resources/city_cleaning.xlsx")
            self.current_status_country_cleaning = read_excel("resources/country_cleaning.xlsx")
            self.current_status_musicbrainz = read_excel("resources/belgian_mscbrnz_artists.xlsx")

    def compare_current_with_old_status(self):
        self.set_datum_vorige_check()
        self.compare_city_cleaning()
        self.compare_country_cleaning()
        self.compare_musicbrainz()

    def compare_city_cleaning(self):
        empty_cleaning = self.current_status_city_cleaning["clean"].isnull()
        self.aantal_ongecleande_steden = len(self.current_status_city_cleaning[empty_cleaning].index)

    def compare_country_cleaning(self):
        empty_cleaning = self.current_status_country_cleaning["clean"].isnull()
        self.aantal_ongecleande_landen = len(self.current_status_country_cleaning[empty_cleaning].index)

    def compare_musicbrainz(self):
        appended = self.current_status_musicbrainz.append(self.old_status_musicbrainz)
        appended.drop_duplicates(keep=False, inplace=True)
        appended.to_excel("resources/belgian_mscbrnz_artists_diff.xlsx")
        fid = self.drive.get_google_drive_id("belgian_mscbrnz_artists_diff.xlsx")
        self.drive.upload_file_to_leeches("resources/belgian_mscbrnz_artists_diff.xlsx", fid)

    def do(self):
        self.set_aantal_musicbrainz_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre()
        self.compare_current_with_old_status()
        self.generate_report()

    def generate_report(self):
        self.report = {
            "recentste_check": self.datum_recentste_check.isoformat(),
            "vorige_check": self.datum_vorige_check,
            "steden": {
                "aantal_ongecleande_steden": self.aantal_ongecleande_steden,
                "link_city_cleaning": self.drive.get_google_drive_link("city_cleaning.xlsx")
            },
            "landen": {
                "aantal_ongecleande_landen": self.aantal_ongecleande_landen,
                "link_country_cleaning": self.drive.get_google_drive_link("country_cleaning.xlsx")
            },
            "concerten": {
                "link_concerten": self.drive.get_google_drive_link("latest.xlsx")
            },
            "genres": {
                "aantal_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre": self.aantal_musicbrainz_artiesten_met_toekomstige_buitenlandse_concerten_zonder_genre,
                "link_mb_belgen": self.drive.get_google_drive_link("belgian_mscbrnz_artists_diff.xlsx")
            },
            "musicbrainz": {
                "totaal_aantal_mb_belgen": len(self.current_status_musicbrainz.index),
                "vorig_aantal_onderzochte_mb_belgen": len(self.old_status_musicbrainz.index),
                "aantal_nieuwe_mb_belgen": len(self.current_status_musicbrainz.index) - len(self.old_status_musicbrainz.index),
                "link_mb_belgen_diff": self.drive.get_google_drive_link("belgian_mscbrnz_artists_diff.xlsx"),
                "link_mb_belgen": self.drive.get_google_drive_link("belgian_mscbrnz_artists_diff.xlsx")
            },
            "ignore_list": {
                "totaal_aantal_genegeerde_mb_belgen": len(self.current_status_ignore.index),
                "vorig_aantal_genegeerd_mb_belgen": len(self.old_status_ignore.index),
                "link_genegeerd_mb_belgen": self.drive.get_google_drive_link("ignore_list.xlsx")
            }
        }

    def send_report(self):
        with open("output/report.json", "w", "utf-8") as f:
            dump(self.report, f, indent=2)
        with open("resources/sendmail/template.mstch", "r", "utf-8") as f:
            template = f.read()

    def set_datum_vorige_check(self):
        try:
            self.datum_vorige_check = self.previous_report["recentste_check"]
        except TypeError:
            self.datum_vorige_check = "Not available"
