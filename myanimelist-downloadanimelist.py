#!/usr/bin/env python

import requests
import bs4
import sqlite3
import logging
import time

database = "anime_list.sqlite3"
logging.basicConfig(
    filename="anime.log", level=logging.DEBUG, format="%(asctime)s %(message)s"
)

anime_list_urls = [
    "https://myanimelist.net/anime.php?letter=.",
    "https://myanimelist.net/anime.php?letter=A",
    "https://myanimelist.net/anime.php?letter=B",
    "https://myanimelist.net/anime.php?letter=C",
    "https://myanimelist.net/anime.php?letter=D",
    "https://myanimelist.net/anime.php?letter=E",
    "https://myanimelist.net/anime.php?letter=F",
    "https://myanimelist.net/anime.php?letter=G",
    "https://myanimelist.net/anime.php?letter=H",
    "https://myanimelist.net/anime.php?letter=I",
    "https://myanimelist.net/anime.php?letter=J",
    "https://myanimelist.net/anime.php?letter=K",
    "https://myanimelist.net/anime.php?letter=L",
    "https://myanimelist.net/anime.php?letter=M",
    "https://myanimelist.net/anime.php?letter=N",
    "https://myanimelist.net/anime.php?letter=O",
    "https://myanimelist.net/anime.php?letter=P",
    "https://myanimelist.net/anime.php?letter=Q",
    "https://myanimelist.net/anime.php?letter=R",
    "https://myanimelist.net/anime.php?letter=S",
    "https://myanimelist.net/anime.php?letter=T",
    "https://myanimelist.net/anime.php?letter=U",
    "https://myanimelist.net/anime.php?letter=V",
    "https://myanimelist.net/anime.php?letter=W",
    "https://myanimelist.net/anime.php?letter=X",
    "https://myanimelist.net/anime.php?letter=Y",
    "https://myanimelist.net/anime.php?letter=Z",
]


def download_a_site(link):
    response = requests.get(link)
    while not response.ok:
        if response.status_code == 429:
            logging.warning("Code 429")
            time.sleep(3600)
        logging.info("SLEEP: " + link)
        time.sleep(100)
        response = requests.get(link)
    return response.text


def run_statment_in_database(database, sql_query):
    cursor = database.cursor()
    cursor.execute(sql_query)
    database.commit()
    return True


def get_all_animes_from_site(data_soup):
    animes = data_soup.find_all("div", class_="js-categories-seasonal")[0].find_all(
        "tr"
    )
    anime_links = [a.find_all("a")[0]["href"] for a in animes]
    anime_keys = [
        link.replace("https://myanimelist.net/anime/", "")
        for link in anime_links
        if "https://myanimelist.net/anime/" in link
    ]
    return (
        "INSERT OR IGNORE INTO anime (anime) VALUES "
        + ",".join(['("' + a + '")' for a in anime_keys])
        + ";"
    )


sql_database = sqlite3.connect(database)

for url in anime_list_urls:
    response_body = download_a_site(url)
    data_soup = bs4.BeautifulSoup(response_body, features="lxml")
    number_of_sites = int(
        data_soup.find_all("div", class_="spaceit")[0].find_all("a")[-1].get_text()
    )
    number_of_entries = number_of_sites * 50
    run_statment_in_database(sql_database, get_all_animes_from_site(data_soup))
    i = 50
    while i < number_of_entries:
        response_body = download_a_site(url + "&show=" + str(i))
        data_soup = bs4.BeautifulSoup(response_body, features="lxml")
        run_statment_in_database(sql_database, get_all_animes_from_site(data_soup))
        i += 50

sql_database.close()
