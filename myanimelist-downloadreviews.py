import requests
import bs4
import datetime
import json
import os
import sqlite3
import logging
import time

project_folder = "/root/myanimelist-review-download-script"
review_folder = project_folder + "/data"
database_file_name = project_folder + "/anime_list.sqlite3"

logging.basicConfig(
    filename=project_folder + "anime.log",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
)


def _get_username_from_link(link):
    if "https://myanimelist.net" in link:
        return link.replace("https://myanimelist.net/profile/", "").split("/")[0]
    if "/profile/" in link:
        return link.replace("/profile/", "").split("/")[0]
    return ""


def _get_review_id(review_element):
    return (
        review_element.find_all("a", class_="lightLink")[0]["href"]
        .split("?")[1]
        .split("=")[1]
    )


def _get_review_text_and_grades_helper(review):
    grade_tables = review.find_all("div", class_="textReadability")[0].find_all(
        "table"
    )[0]
    table_element = grade_tables.replace_with("")
    main_grade_element = review.find_all("div", class_="mb8")[0].find_all("div")[2]
    grades = _get_grades_from_table(table_element, main_grade_element)
    review_text = (
        review.find_all("div", class_="textReadability")[0]
        .get_text()
        .strip()
        .replace("Helpful\n\n\nread more", "")
    )
    return review_text, grades


def _get_grades_from_table(table_element, main_grade_element):
    grades = dict()
    grades["Main"] = main_grade_element.get_text().strip().split(":")[1].strip()
    grades_in_lists = [
        [e.get_text() for e in row.find_all("td")]
        for row in table_element.find_all("tr")
    ]
    for name, grade in grades_in_lists:
        grades[name.strip()] = grade.strip()
    return grades


def _get_date_of_review(review):
    review_metadata = review.find_all("div", class_="mb8")[0].find_all("div")
    return (
        datetime.datetime.strptime(review_metadata[0].get_text(), "%b %d, %Y")
        .isoformat()
        .split("T")[0]
    )


def _get_author_username(review):
    user_link = review.find_all("table")[0].find_all("a")[0]["href"]
    return _get_username_from_link(user_link)


def get_review_info(review, series_name):
    author = _get_author_username(review)
    date = _get_date_of_review(review)
    text, grades = _get_review_text_and_grades_helper(review)
    review_id = _get_review_id(review)
    return {
        "id": review_id,
        "series": series_name,
        "author": author,
        "date": date,
        "grades": grades,
        "text": text,
    }


def save_json_to_file(json_to_save, folder):
    with open(os.path.join(folder, json_to_save["id"] + ".json"), "w") as f:
        json.dump(json_to_save, f)


def get_next_page(number_of_reviews_on_site, current_page_index):
    if number_of_reviews_on_site == 20:
        return str(int(current_page_index) + 1)
    return False


def process_a_website_of_reviews(series, next_page_index, review_folder):
    response_body = download_a_website_of_reviews(series, next_page_index)
    data_soup = bs4.BeautifulSoup(response_body, features="lxml")
    reviews = data_soup.find_all("div", class_="borderDark")
    processed_reviews = [get_review_info(review, series) for review in reviews]
    [save_json_to_file(review_info, review_folder) for review_info in processed_reviews]
    next_page_index = get_next_page(len(processed_reviews), next_page_index)
    return next_page_index


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


def download_a_website_of_reviews(series, page_index):
    review_link = "https://myanimelist.net/anime/" + series + "/reviews?p=" + page_index
    return download_a_site(review_link)


# this is the main function for processing all the reviews of the series
def process_the_reviews_for_series(series, review_folder):
    next_page_index = process_a_website_of_reviews(series, "1", review_folder)
    while next_page_index:
        next_page_index = process_a_website_of_reviews(
            series, next_page_index, review_folder
        )
    return True


def run_statment_in_database(database, sql_query):
    cursor = database.cursor()
    cursor.execute(sql_query)
    database.commit()
    return list(cursor.fetchall())


def process_anime(sql_database, review_folder):
    next_anime_sql = run_statment_in_database(
        sql_database, "SELECT anime FROM anime WHERE finished IS NULL LIMIT 1;"
    )
    if len(next_anime_sql):
        next_anime = next_anime_sql[0][0]
        process_the_reviews_for_series(next_anime, review_folder)
        run_statment_in_database(
            sql_database,
            'UPDATE anime SET finished = DATE("now") WHERE anime = "'
            + next_anime
            + '";',
        )
        process_anime(sql_database, review_folder)
    return True


def __main__(database_file_name, review_folder):
    sql_database = sqlite3.connect(database_file_name)
    process_anime(sql_database, review_folder)
    sql_database.close()


__main__(database_file_name, review_folder)
