import requests
import bs4
import datetime
import json
import os
import sqlite3
import logging
import time

project_folder = "/root/myanimelist-review-download-script"
metadata_folder = project_folder + "/metadata"
database_file_name = project_folder + "/anime_list.sqlite3"

logging.basicConfig(
    filename=project_folder + "anime.log",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
)

default_list_elements = [
    ["producers", "Producers:"],
    ["license_holders", "Licensors:"],
    ["studios", "Studios:"],
]
default_elements = [
    ["type", "Type:"],
    ["number_of_episodes", "Episodes:"],
    ["period__running", "Aired:"],
    ["starting_season", "Premiered:"],
    ["source", "Source:"],
    ["duration", "Duration:"],
    ["raiting", "Rating:"],
    ["popularity", "Popularity:"],
    ["number_of_members", "Members:"],
    ["number_of_favorites", "Favorites:"],
]


def save_json_to_file(json_to_save, folder, anime):
    with open(os.path.join(folder, anime.replace("/", "_") + ".json"), "w") as f:
        json.dump(json_to_save, f)


def download_a_website_of_reviews(series):
    review_link = "https://myanimelist.net/anime/" + series + "/reviews?p=1"
    return download_a_site(review_link)


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


def get_all_series_names(data_soup):
    names = []
    whole_name = data_soup.find_all("span", class_="h1-title")[0].get_text()
    eng_title_elements = data_soup.find_all("span", class_="title-english")
    if len(eng_title_elements):
        eng_title = eng_title_elements[0].get_text()
        names.append(eng_title)
        names.append(whole_name.replace(eng_title, ""))
    else:
        names.append(whole_name)
    alternative_names_elements = data_soup.find_all("div", class_="spaceit_pad")
    return names + [
        e.get_text().split(":")[1].strip() for e in alternative_names_elements
    ]


def get_default_element(element, field_string):
    element = [e for e in element.find_all("div") if field_string in e.get_text()]
    if len(element) < 2:
        return ""
    return element[1].get_text().split(":")[1].strip()


def get_series_id_and_key_name(series_name):
    series_id, series_key = series_name.split("/")
    return [["id", series_id], ["series", series_key]]


def get_default_element_list(element, field_string):
    return [e.strip() for e in get_default_element(element, field_string).split(",")]


def get_genres_element(element):
    return [
        [
            "genre",
            [
                g.get_text()
                for g in [
                    e for e in element.find_all("div") if "Genres:" in e.get_text()
                ][1].find_all("span")[1:]
            ],
        ]
    ]


def get_score_element(element):
    return [
        [
            "score",
            [
                e
                for e in element.find_all("div")
                if "Score:" in e.get_text() and "scored by" in e.get_text()
            ][1]
            .get_text()
            .split(":")[1]
            .split("(")[0][:-2]
            .strip(),
        ]
    ]


# this is function for producing series metadata
def process_series_metadata(series_name, data_soup, metadata_folder):
    series_id_list = get_series_id_and_key_name(series_name)
    names_in_list = [["name", get_all_series_names(data_soup)]]
    sidebar_element = data_soup.find_all("td", class_="borderClass")[0]
    metadata_list = (
        [
            (name, get_default_element(sidebar_element, s))
            for name, s in default_elements
        ]
        + [
            (name, get_default_element_list(sidebar_element, s))
            for name, s in default_list_elements
        ]
        + get_score_element(sidebar_element)
        + get_genres_element(sidebar_element)
    )
    date = [["date_saved", datetime.datetime.now().isoformat().split("T")[0]]]
    anime_metadata = dict(
        series_id_list + date + names_in_list + metadata_list + [["key", series_name]]
    )
    return save_json_to_file(anime_metadata, metadata_folder, series_name)


def run_statment_in_database(database, sql_query):
    cursor = database.cursor()
    cursor.execute(sql_query)
    database.commit()
    return list(cursor.fetchall())


def process_the_reviews_for_series(series, metadata_folder):
    response_body = download_a_website_of_reviews(series)
    data_soup = bs4.BeautifulSoup(response_body, features="lxml")
    return process_series_metadata(series, data_soup, metadata_folder)


def process_anime(sql_database, metadata_folder):
    next_anime_sql = run_statment_in_database(
        sql_database, "SELECT anime FROM anime WHERE finished_metadata IS NULL LIMIT 1;"
    )
    if len(next_anime_sql):
        next_anime = next_anime_sql[0][0]
        process_the_reviews_for_series(next_anime, metadata_folder)
        run_statment_in_database(
            sql_database,
            'UPDATE anime SET finished_metadata = DATE("now") WHERE anime = "'
            + next_anime
            + '";',
        )
        process_anime(sql_database, metadata_folder)
    return True


def __main__(database_file_name, metadata_folder):
    sql_database = sqlite3.connect(database_file_name)
    process_anime(sql_database, metadata_folder)
    sql_database.close()


__main__(database_file_name, metadata_folder)
