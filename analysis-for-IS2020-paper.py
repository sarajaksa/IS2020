import sqlite3
import collections
import pandas
import numpy
import statsmodels.stats.proportion as proportion
from statsmodels.stats.weightstats import ttest_ind
import statistics

database_file_name = "data.db"

database = sqlite3.connect(database_file_name)
cursor = database.cursor()

sql_query = "SELECT * FROM review"
cursor.execute(sql_query)
reviews = cursor.fetchall()

sql_query = "SELECT * FROM genres"
cursor.execute(sql_query)
genres = cursor.fetchall()

sql_query = "SELECT * FROM personality"
cursor.execute(sql_query)
personality = cursor.fetchall()

database.close()

lower_limit = int(len(personality) / 3)
upper_limit = len(personality) - int(len(personality) / 3)

c_upper_limit = sorted([trait[2] for trait in personality])[upper_limit]
c_lower_limit = sorted([trait[2] for trait in personality])[lower_limit]
a_upper_limit = sorted([trait[3] for trait in personality])[upper_limit]
a_lower_limit = sorted([trait[3] for trait in personality])[lower_limit]
o_upper_limit = sorted([trait[4] for trait in personality])[upper_limit]
o_lower_limit = sorted([trait[4] for trait in personality])[lower_limit]
e_upper_limit = sorted([trait[5] for trait in personality])[upper_limit]
e_lower_limit = sorted([trait[5] for trait in personality])[lower_limit]
n_upper_limit = sorted([trait[6] for trait in personality])[upper_limit]
n_lower_limit = sorted([trait[6] for trait in personality])[lower_limit]

c_authors_higher = set([trait[0] for trait in personality if trait[2] > c_upper_limit])
c_authors_lower = set([trait[0] for trait in personality if trait[2] < c_lower_limit])
a_authors_higher = set([trait[0] for trait in personality if trait[3] > a_upper_limit])
a_authors_lower = set([trait[0] for trait in personality if trait[3] < a_lower_limit])
o_authors_higher = set([trait[0] for trait in personality if trait[4] > o_upper_limit])
o_authors_lower = set([trait[0] for trait in personality if trait[4] < o_lower_limit])
e_authors_higher = set([trait[0] for trait in personality if trait[5] > e_upper_limit])
e_authors_lower = set([trait[0] for trait in personality if trait[5] < e_lower_limit])
n_authors_higher = set([trait[0] for trait in personality if trait[6] > n_upper_limit])
n_authors_lower = set([trait[0] for trait in personality if trait[6] < n_lower_limit])

all_genres = set()
anime_genres = collections.defaultdict(list)
for genre, anime in genres:
    anime_genres[anime].append(genre)
    all_genres.add(genre)

all_reviews_for_group = [
    [author, anime, grade]
    for author, anime, grade in reviews
    if author in c_authors_higher
]

reviews_for_genre = [
    [author, anime, grade]
    for author, anime, grade in all_reviews_for_group
    if genre in anime_genres[anime]
]

authors = [
    [c_authors_higher, c_authors_lower],
    [a_authors_higher, a_authors_lower],
    [o_authors_higher, o_authors_lower],
    [e_authors_higher, e_authors_lower],
    [n_authors_higher, n_authors_lower],
]
traits = [
    "Conscientiousness",
    "Agreeableness",
    "Openness",
    "Extraversion",
    "Neuroticism",
]


# Analysis 1 - Number of Reviews Written


def get_statistical_results_number_of_review(
    trait, reviews, authors_higher, authors_lower
):
    all_data = []
    all_reviews_for_group_higher = [
        [author, anime, grade]
        for author, anime, grade in reviews
        if author in authors_higher
    ]
    all_reviews_for_group_lower = [
        [author, anime, grade]
        for author, anime, grade in reviews
        if author in authors_lower
    ]
    number_of_all_reviews_higher = len(all_reviews_for_group_higher)
    number_of_all_reviews_lower = len(all_reviews_for_group_lower)
    for genre in all_genres:
        reviews_for_genre_higher = [
            [author, anime, grade]
            for author, anime, grade in all_reviews_for_group_higher
            if genre in anime_genres[str(anime)]
        ]
        reviews_for_genre_lower = [
            [author, anime, grade]
            for author, anime, grade in all_reviews_for_group_lower
            if genre in anime_genres[str(anime)]
        ]
        ratio_higher = (
            len(reviews_for_genre_higher) / number_of_all_reviews_higher
        ) * 100
        ratio_lower = (len(reviews_for_genre_lower) / number_of_all_reviews_lower) * 100
        diff_ratio = ratio_higher - ratio_lower
        chisq, pvalue, table = proportion.proportions_chisquare(
            [len(reviews_for_genre_higher), len(reviews_for_genre_lower)],
            [number_of_all_reviews_higher, number_of_all_reviews_lower],
        )
        power = chisq / (number_of_all_reviews_higher + number_of_all_reviews_lower)
        all_data.append(
            [
                genre,
                number_of_all_reviews_higher,
                len(reviews_for_genre_higher),
                number_of_all_reviews_lower,
                len(reviews_for_genre_lower),
                ratio_higher,
                ratio_lower,
                diff_ratio,
                chisq,
                pvalue,
                power,
                trait,
            ]
        )
    return all_data


def get_all_results_number_of_reviews(reviews, traits, authors, filename):
    results = []
    for trait, authors in zip(traits, authors):
        authors_higher, authors_lower = authors
        results = results + get_statistical_results_number_of_review(
            trait, reviews, authors_higher, authors_lower
        )
    result_pandas = pandas.DataFrame(
        results,
        columns=[
            "genre",
            "num higher",
            "rew higher",
            "num lower",
            "rew lower",
            "ratio higher",
            "ratio lower",
            "diff",
            "chi",
            "p",
            "power",
            "trait",
        ],
    )
    result_csv = result_pandas.to_csv()
    with open(filename, "w") as f:
        f.write(result_csv)
    return True


get_all_results_number_of_reviews(
    reviews, traits, authors, "statistical_test_1_results.csv"
)


# # Analysis 2 - The Scores of the Reviews


def cohend(mean1, mean2, array1, array2):
    return abs(mean1 - mean2) / statistics.sqrt(
        (numpy.std(array1) ** 2 + numpy.std(array2) ** 2) / 2
    )


def get_result_data_scores_of_reviews(trait, reviews, authors_higher, authors_lower):
    all_data = []
    all_reviews_for_group_higher = [
        [author, anime, grade]
        for author, anime, grade in reviews
        if author in authors_higher
    ]
    all_reviews_for_group_lower = [
        [author, anime, grade]
        for author, anime, grade in reviews
        if author in authors_lower
    ]
    for genre in all_genres:
        reviews_for_genre_higher = [
            grade
            for author, anime, grade in all_reviews_for_group_higher
            if genre in anime_genres[str(anime)]
        ]
        reviews_for_genre_lower = [
            grade
            for author, anime, grade in all_reviews_for_group_lower
            if genre in anime_genres[str(anime)]
        ]
        avg_score_higher = sum(reviews_for_genre_higher) / len(reviews_for_genre_higher)
        avg_score_lower = sum(reviews_for_genre_lower) / len(reviews_for_genre_lower)
        t, p, df = ttest_ind(reviews_for_genre_higher, reviews_for_genre_lower)
        cd = cohend(
            avg_score_higher,
            avg_score_lower,
            reviews_for_genre_higher,
            reviews_for_genre_lower,
        )
        diff = avg_score_higher - avg_score_lower
        all_data.append(
            [genre, avg_score_higher, avg_score_lower, diff, t, df, p, cd, trait]
        )
    return all_data


def get_all_results_score_of_reviews(reviews, traits, authors, filename):
    results = []
    for trait, authors in zip(traits, authors):
        authors_higher, authors_lower = authors
        results = results + get_result_data_scores_of_reviews(
            trait, reviews, authors_higher, authors_lower
        )
    results_pandas = pandas.DataFrame(
        results,
        columns=[
            "genre",
            "avg higher",
            "avg lower",
            "diff",
            "t",
            "df",
            "p",
            "cd",
            "trait",
        ],
    )
    result_csv = results_pandas.to_csv()
    with open(filename, "w") as f:
        f.write(result_csv)
    return True


get_all_results_score_of_reviews(
    reviews, traits, authors, "statistical_test_2_results.csv"
)
