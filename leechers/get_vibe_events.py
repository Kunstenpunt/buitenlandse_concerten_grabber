from requests import get, exceptions
from codecs import open
from bs4 import BeautifulSoup
from json import loads
from re import compile
from datetime import datetime
from pandas import DataFrame
from time import sleep


def get_html(url):
    try:
        return get(url).text
    except exceptions:
        sleep(5.0)
        return get_html(url)

base_url = "http://vi.be"

all_data = []

with open("20170320_vibe.html", "r", "utf-8") as f:
    soup = BeautifulSoup(f.read(), "html.parser")
    lijst_van_vibers = [(c["href"], c["alt"]) for c in soup.find_all("a", attrs={"class": "card-content"})]

for viber, naam in lijst_van_vibers:
    print(naam)
    vibe_response = get_html(base_url + viber)
    vibe_pagina = BeautifulSoup(vibe_response, "html.parser")
    past_shows = loads(get(base_url + viber + "/shows/past").text)
    upcoming_shows = loads(get(base_url + viber + "/shows/upcoming").text)
    for show in past_shows["shows"] + upcoming_shows["shows"]:
        for genre in compile('<span class="hm-item"><i class="icon icon-music"></i>(.*?)</span><br>').findall(vibe_response)[0].split("&bull;"):
            data = {
                "venue": show["name"].strip(),
                "stad": show["location"].replace(compile("(\(\w*?\))").findall(show["location"])[-1], "").strip(),
                "land": compile("\((\w*?)\)").findall(show["location"])[0].strip(),
                "latitude": show["locationLat"],
                "longitude": show["locationLng"],
                "datum": datetime.fromtimestamp(float((show["date"]))/1000.0).date() if "date" in show else "NA",
                "artiest": naam,
                "artiest_land": compile("\((\w*?)\)").findall(vibe_pagina.find("span", attrs={"class": "hm-item"}).text)[-1],
                "artiest_genre": genre.strip(),
                "source": "Vibe"
            }
            all_data.append(data)

DataFrame(all_data).to_excel("providers/vibe.xlsx", index=False)
