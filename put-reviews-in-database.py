import os
import json
import sqlite3

database_file_name = "data.db"
folder_name = "data"

all_review_files = os.listdir(folder_name)

database = sqlite3.connect(database_file_name)
cursor = database.cursor()
for filename in all_review_files:
    with open(os.path.join("data", filename)) as f:
        data = json.load(f)
    series = data["series"].split("/")[0]
    author = data["author"]
    main_grade = data["grades"]["Main"]
    sql_query = f'INSERT INTO review (author, anime, grade) values ("{author}", {series}, {main_grade})'
    cursor.execute(sql_query)
database.commit()
database.close()
