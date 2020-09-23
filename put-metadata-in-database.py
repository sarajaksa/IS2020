import os
import json
import sqlite3
import re

folder = "metadata"
database_file_name = "data.db"


def get_year_of_start(anime_data):
    re_year = r"\d\d\d\d"

    def get_year_from_string(string):
        all_years = re.findall(re_year, string)
        if len(all_years):
            return all_years[0]
        return False

    return (
        get_year_from_string(anime_data["starting_season"])
        or get_year_from_string(anime_data["period__running"])
        or ""
    )


def get_duration_in_string(anime_data):
    if anime_data["duration"] == "Unknown":
        return ""
    duration = 0
    duration_string = anime_data["duration"].replace("per ep.", "").strip()
    minutes = re.findall(r"\d+? min", duration_string)
    if len(minutes):
        duration += int(minutes[0].replace("min", "").strip())
    hours = re.findall(r"\d+? hr", duration_string)
    if len(hours):
        duration += int(hours[0].replace("hr", "").strip()) * 60
    return str(duration)


database = sqlite3.connect(database_file_name)
cursor = database.cursor()
for filename in os.listdir(folder):
    with open(os.path.join(folder, filename)) as f:
        data = json.load(f)
    anime = data["id"]
    year = get_year_of_start(data)
    duration = get_duration_in_string(data)
    popularity = data["popularity"].replace("#", "")
    sql_data = (
        '"'
        + '", "'.join(
            [
                data["id"],
                data["series"],
                data["type"],
                data["number_of_episodes"],
                year,
                data["source"],
                duration,
                data["raiting"],
                data["score"],
                popularity,
                data["number_of_members"],
                data["number_of_favorites"],
            ]
        )
        + '"'
    )
    sql_query = f"INSERT INTO anime (id, name, type, episodes, year, source, duration, raiting, score, popularity, members, favorites) VALUES ({sql_data})"
    cursor.execute(sql_query)
    this_anime_genres = data["genre"]
    for genre in this_anime_genres:
        sql_query = f'INSERT INTO genres VALUES ("{genre}", "{anime}")'
        cursor.execute(sql_query)
    anime_producers = data["producers"]
    for producer in anime_producers:
        if producer != "None found" and producer != "add some":
            sql_query = f'INSERT INTO producers (anime, producer) VALUES ("{anime}", "{producer}")'
            cursor.execute(sql_query)
    license_holders = data["license_holders"]
    for license_holder in license_holders:
        if license_holder != "None found" and license_holder != "add some":
            sql_query = f'INSERT INTO license_holder (anime, license) VALUES ("{anime}", "{license_holder}")'
            cursor.execute(sql_query)
    studios = data["studios"]
    for studio in studios:
        if studio != "None found" and studio != "add some":
            sql_query = (
                f'INSERT INTO studios (anime, studio) VALUES ("{anime}", "{studio}")'
            )
            cursor.execute(sql_query)

database.commit()
database.close()
