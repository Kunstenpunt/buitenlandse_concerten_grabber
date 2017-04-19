from requests import get, exceptions
from bs4 import BeautifulSoup
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


def get_datum(event):
    return datetime.strptime(event.find("time")["datetime"], "%Y-%m-%dT%H:%M").date()


def get_land(event):
    return event.find("span", attrs={"itemprop": "country-name"}).text


def get_titel(event):
    return event.find("a", attrs={"itemprop": "url summary"}).text


def get_venue(event):
    return event.find("span", attrs={"itemprop": "name"}).text


def get_stad(event):
    href = event.find("a", href=compile("/club.aspx\?id=\d+"))
    if href:
        html = get_html("https://www.residentadvisor.net" + href["href"])
        soup = BeautifulSoup(html, "html.parser")
        find = soup.find("span", attrs={"itemprop": "street-address"})
        if find:
            return find.text


def get_latitude(event):
    href = event.find("a", href=compile("/club.aspx\?id=\d+"))
    if href:
        html = get_html("https://www.residentadvisor.net" + href["href"])
        soup = BeautifulSoup(html, "html.parser")
        find = soup.find("input", attrs={"id": "hdnLatitude"})
        if find:
            return float(find["value"])


def get_longitude(event):
    href = event.find("a", href=compile("/club.aspx\?id=\d+"))
    if href:
        html = get_html("https://www.residentadvisor.net" + href["href"])
        soup = BeautifulSoup(html, "html.parser")
        find = soup.find("input", attrs={"id": "hdnLongitude"})
        if find:
            return float(find["value"])


url = "https://www.residentadvisor.net/dj.aspx?area=62"

lines = []

soup = BeautifulSoup(get(url).text, "html.parser")
for dj in soup.find_all("a", href=compile(r'/dj/.+?')):
    if dj["href"] != "/dj/favourites":
        print(dj["href"])
        dj_url = "https://www.residentadvisor.net" + dj["href"] + "/dates"
        soup_dj = BeautifulSoup(get(dj_url).text, "html.parser")
        for event in soup_dj.find_all("article", attrs={"class": "event"}):
            out = {
                "artiest": "/".join(dj["href"].split("/")[2:]),
                "datum": get_datum(event),
                "land": get_land(event),
                "stad": get_stad(event),
                "latitude": get_latitude(event),
                "longitude": get_longitude(event),
                "source": "Resident Advisor",
                "titel": get_titel(event),
                "venue": get_venue(event)
            }
            lines.append(out)

DataFrame(lines).drop_duplicates(inplace=False).to_excel("providers/residentadvisor.xlsx", index=False)
