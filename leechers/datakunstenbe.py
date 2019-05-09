from configparser import ConfigParser
import psycopg2
from pandas import DataFrame, Timestamp
from datetime import datetime


class DataKunstenBeConnector(object):
    def __init__(self, root="./leechers/resources/"):
        cfg = ConfigParser()
        cfg.read(root + "resources/db.cfg")
        knst = psycopg2.connect(host=cfg['db']['host'], port=cfg['db']['port'],
                                database=cfg['db']['db'], user=cfg['db']['user'],
                                password=cfg['db']['pwd'])
        knst.set_client_encoding('UTF-8')
        self.cur = knst.cursor()
        self.concerts = None

    def get_concerts_abroad(self):
        sql = """
            SELECT
              s.id, d.year, d.month, d.day,
              p.full_name artiest_volledige_naam,
              o.name organisatie_naam, o.city organisatie_stad, o_countries.iso_code organisatie_land,
              l_organisations.city_nl organisatie_locatie_stad, l_organisations_countries.iso_code organisatie_locatie_land,
              l_a_organisations.city_nl organisatie_adres_locatie_stad, l_a_organisations_countries.iso_code organisatie_adres_locatie_land,
              v.name venue_naam, l_venues.city_nl venue_locatie_stad, l_venues_countries.iso_code venue_locatie_land

            FROM production.shows s

            JOIN production.show_types st
            ON s.show_type_id = st.id

            JOIN production.date_isaars d
            ON s.date_id = d.id

            JOIN production.relationships rel_show_person
            ON s.id = rel_show_person.show_id
            JOIN production.people p
            ON rel_show_person.person_id = p.id

            LEFT JOIN production.countries p_countries
            ON p.country_id = p_countries.id

            LEFT JOIN production.locations p_locations
            ON p.location_id = p_locations.id
            LEFT JOIN production.countries p_locations_countries
            ON p_locations.country_id = p_locations_countries.id

            LEFT JOIN production.locations p_c_locations
            ON p.current_location_id = p_c_locations.id
            LEFT JOIN production.countries p_c_locations_countries
            ON p_c_locations.country_id = p_c_locations_countries.id

            JOIN production.organisations o
            ON s.organisation_id = o.id
            LEFT JOIN production.countries o_countries
            ON o.country_id = o_countries.id
            LEFT JOIN production.locations l_organisations
            ON o.location_id = l_organisations.id
            LEFT JOIN production.countries l_organisations_countries
            ON l_organisations.country_id = l_organisations_countries.id

            LEFT JOIN production.locations l_a_organisations
            ON o.address_location_id = l_a_organisations.id
            LEFT JOIN production.countries l_a_organisations_countries
            ON l_a_organisations.country_id = l_a_organisations_countries.id

            JOIN production.relationships rel_show_genre
            ON s.id = rel_show_genre.show_id
            JOIN production.genres g
            ON rel_show_genre.genre_id = g.id

            LEFT JOIN production.venues v
            ON s.venue_id = v.id

            LEFT JOIN production.countries v_countries
            ON v.country_id = v_countries.id
            LEFT JOIN production.locations l_venues
            ON v.location_id = l_venues.id
            LEFT JOIN production.countries l_venues_countries
            ON l_venues.country_id = l_venues_countries.id

            WHERE st.id IN (456, 457)

            ORDER BY
              d.year DESC, d.month DESC, d.day DESC
        """
        self.cur.execute(sql)
        os = self.cur.fetchall()
        df = DataFrame(os, columns=["id", "year", "month", "day", "artiest", "organisatie_naam", "organisatie_stad1", "organisatie_land1", "organisatie_stad2", "organisatie_land2", "organisatie_stad3", "organisatie_land3", "venue_naam", "venue_stad", "venue_land"])
        df["event_id"] = ["datakunstenbe_" + str(row[1]["id"]) for row in df.iterrows()]
        df["datum"] = [Timestamp(datetime(int(row[1]["year"]), int(row[1]["month"]), int(row[1]["day"]) if row[1]["day"] > 0 else 1).date()) for row in df.iterrows()]
        df["stad"] = [row[1]["venue_stad"] if row[1]["venue_stad"] is not None else row[1]["organisatie_stad3"] if row[1]["organisatie_stad3"] is not None else row[1]["organisatie_stad2"] if row[1]["organisatie_stad2"] is not None else row[1]["organisatie_stad1"] for row in df.iterrows()]
        df["land"] = [row[1]["venue_land"] if row[1]["venue_land"] else row[1]["organisatie_land3"] if row[1]["organisatie_land3"] else row[1]["organisatie_land2"] if row[1]["organisatie_land2"] else row[1]["organisatie_land1"] for row in df.iterrows()]
        df["venue"] = [row[1]["venue_naam"] if row[1]["venue_naam"] else row[1]["organisatie_naam"] for row in df.iterrows()]
        df["source"] = ["datakunstenbe"] * len(df.index)
        df["artiest_mb_naam"] = df["artiest"]
        self.concerts = df.drop(labels=["id", "year", "month", "day", "organisatie_naam", "organisatie_stad1", "organisatie_land1", "organisatie_stad2", "organisatie_land2", "organisatie_stad3", "organisatie_land3", "venue_naam", "venue_stad", "venue_land"], axis=1)


if __name__ == "__main__":
    leecher = DataKunstenBeConnector(root="./")
    leecher.get_concerts_abroad()
    leecher.concerts.to_excel("output/" + datetime.now().date().isoformat() + "_datakunstenbe.xlsx")
