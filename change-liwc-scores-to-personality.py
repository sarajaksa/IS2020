import pandas
import sqlite3

database_file_name = "data.db"
filename = "LIWC2015 Results (users (52235 files)).csv"


def number(n):
    return float(n.replace(",", "."))


def put_personality_to_database(row, cursor):
    sql_query = f'INSERT INTO personality (author, WC, C, A, O, E, N) values ("{row.Filename}", {row.WC}, {row.C}, {row.A}, {row.O}, {row.E}, {row.N})'
    cursor.execute(sql_query)


with open(filename) as f:
    data = f.readlines()

data_split = [v.replace(",", ";", 2).replace("\n", "").split(";") for v in data]
pandas_data = pandas.DataFrame(data_split[1:], columns=data_split[0])

pandas_data["C"] = pandas_data.apply(
    lambda row: number(row.achieve)
    - number(row.anger)
    - number(row.negemo)
    - number(row.negate),
    axis=1,
)
pandas_data["A"] = pandas_data.apply(
    lambda row: -number(row.swear)
    + number(row.home)
    + number(row.leisure)
    + number(row.motion)
    + number(row.space)
    - number(row.anger)
    - number(row.negemo)
    + number(row.posemo),
    axis=1,
)
pandas_data["O"] = pandas_data.apply(
    lambda row: +number(row.death)
    - number(row.home)
    - number(row.leisure)
    - number(row.motion)
    - number(row.time)
    - number(row.family)
    - number(row.social)
    - number(row.posemo)
    + number(row.prep)
    + number(row.article)
    - number(row.i)
    - number(row.pronoun),
    axis=1,
)
pandas_data["E"] = pandas_data.apply(
    lambda row: +number(row.sexual)
    + number(row.friend)
    + number(row.social)
    + number(row.you),
    axis=1,
)
pandas_data["N"] = pandas_data.apply(
    lambda row: number(row.anx) + number(row.negemo) - number(row.you), axis=1
)

database = sqlite3.connect(database_file_name)
cursor = database.cursor()
pandas_data.apply(lambda row: put_personality_to_database(row, cursor), axis=1)
database.commit()
database.close()
