from json import dumps
import hashlib
import hmac
import binascii
from pandas import isnull, Timestamp, read_excel
from requests import post, exceptions
from datetime import datetime, date
from time import sleep


class HaveLoveWillTravel(object):
    def __init__(self, root="./"):
        self.df = None
        self.diff_event_ids = None
        self.root = root

    def set_data(self, df, diff):
        self.df = df
        self.diff_event_ids = diff

    def send_data_to_mr_henry(self, test=False):
        df_filtered = self.df[(self.df["iso_code_clean"] != "BE") & (self.df["datum"] >= datetime(2010, 1, 1)) & (self.df["event_id"].isin(self.diff_event_ids))]
        for record in df_filtered.to_dict("records"):
            print(record)
            self._send_record_to_mr_henry_api(record, test=test)

    @staticmethod
    def json_serial(obj):
        if isinstance(obj, (datetime, date, Timestamp)):
            return obj.strftime("%Y/%m/%d")
        raise TypeError("Type %s not serializable" % type(obj))

    def _send_record_to_mr_henry_api(self, data, test=False):
        data = {key: data[key] for key in data if not isnull(data[key])}
        message = bytes(dumps(data, default=self.json_serial), "utf-8")

        print(message)

        with open(self.root + "havelovewilltravel/resources/mrhenrysecret.txt", "rb") as f:
            secret = bytes(f.read().strip())
            print(secret)

        signature = binascii.b2a_hex(hmac.new(secret, message, digestmod=hashlib.sha256).digest())

        base_url = "https://have-love-will-travel.herokuapp.com/"
        url = base_url + "import-json"

        params = {"signature": signature, "test": test}
        headers = {"Content-Type": "application/json"}

        r = None
        try:
            r = post(url, data=message, params=params, headers=headers)
            if r.status_code != 200:
                print("issue with sending this record to the api", message, r.status_code, r.headers)
        except exceptions.ConnectionError:
            sleep(5)
            self._send_record_to_mr_henry_api(data, test)
        return r


if __name__ == '__main__':
    hlwt = HaveLoveWillTravel()
    df = read_excel("output/latest.xlsx")
    df_filtered = df[df["visible"] & -df["ignore"]]
    for record in df_filtered.to_dict("records"):
        print(record)
        hlwt._send_record_to_mr_henry_api(record, test=False)