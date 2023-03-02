import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pylab
import json
import mysql.connector
import re
from datetime import datetime
import warnings
from pymongo import MongoClient

AUTH = os.environ.get('DB_MONGODB_IMDB')
cluster = MongoClient(AUTH)
db = cluster["test"]
collection = db["imdbs"]

warnings.filterwarnings('ignore')


def transform_to_heatmap(dataframe, dataframe2):
    df_ratings = dataframe.sort_values(by=["seasonNumber", "episodeNumber"], ascending=[True, True])
    df_votes = dataframe2.sort_values(by=["seasonNumber", "episodeNumber"], ascending=[True, True])
    heatmap_container = []
    heatmap_container2 = []
    for episode_index, v in df_ratings.iterrows():
        heatmap_container.append({
            "id": "S" + str(int(v[0])),
            "data": [
                {
                    "x": "E" + str(int(v[1])),
                    "y": float(v[2])
                }
            ]
        })
    for episode_index, v in df_votes.iterrows():
        heatmap_container2.append({
            "id": "S" + str(int(v[0])),
            "data": [
                {
                    "x": "E" + str(int(v[1])),
                    "y": float(v[2])
                }
            ]
        })

    return heatmap_container, heatmap_container2


def imdb_series_metrics(dataframe, tconst, series_name):
    return {
        'tconst': tconst,
        'name': series_name,
        'average_rating': float(round(dataframe['averageRating'].mean(), 2)),
        'total_hours': float(round(dataframe['runtimeMinutes'].sum() / 60, 2)),
        'min_score': float(round(dataframe['averageRating'].min(), 2)),
        'max_score': float(round(dataframe['averageRating'].max(), 2)),
        'std_rating': float(round(dataframe['averageRating'].std(), 2)),
        'min_votes': float(round(dataframe['numVotes'].min(), 2)),
        'max_votes': float(round(dataframe['numVotes'].max(), 2)),
        'average_votes': float(round(dataframe['numVotes'].mean(), 2)),
        'std_votes': float(round(dataframe['numVotes'].std(), 2)),
        'total_votes': float(round(dataframe['numVotes'].sum(), 2)),
    }


def serialize_json(result_matrix, result_metrics, title):
    clean_title = "_".join(re.findall(r'[A-Za-z0-9]+', title))
    # upload = json.dumps(
    #     {

    #     })

    collection.insert_one({"ratings": result_matrix[0],
                           "votes": result_matrix[1],
                           "metrics": result_metrics})
    print(f"Successfully uploaded {title}!")

    # with open(f"./heatmap_parsed/HEATMAP_{clean_title}.json", "w") as f:
    #     json.dump({
    #         "ratings": result_matrix[0],
    #         "votes": result_matrix[1],
    #         "metrics": result_metrics
    #     }, f, indent=4, ensure_ascii=False)
    # with open(f"./heatmap_parsed/HEATMAP_{clean_title}_METRICS.json", "w") as f:
    #     json.dump(result_metrics, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    START_ZERO = datetime.now()
    db_user = os.environ.get('DB_USER_imdb')
    db_pw = os.environ.get('DB_PASSWORD_imdb')
    connection = mysql.connector.connect(host="localhost", user=db_user, passwd=db_pw, database="imdb_march_2023")

    test_series = pd.read_json("./data/daniel_series_watched.json").T

    print("SERIES TO PARSE: ", len(test_series))
    for tconst, title in test_series[0].items():
        print("Starting: ", title)
        try:
            print(datetime.today(), " : ", title, " started...")

            START = datetime.now()

            SERIES_QUERY = title
            print(f"Searching for {title}...")

            q1 = "SELECT a.tconst, parentTconst, seasonNumber, episodeNumber, startYear,averageRating ,numVotes,primaryTitle, runtimeMinutes FROM title_episode_cleaned a JOIN title_ratings b ON a.tconst = b.tconst JOIN title_basics c ON a.tconst = c.tconst WHERE parentTconst = '" + tconst + "'"

            ga = pd.read_sql_query(q1, connection)
            # ga2 = pd.read_sql_query(q2, connection)
            ga.fillna(0, inplace=True)
            ga['runtimeMinutes'] = ga['runtimeMinutes'].astype('int')
            ga['startYear'] = ga['startYear'].astype('int64')
            df = ga.copy()
            df2 = ga.copy()
            df1 = df[['seasonNumber', 'episodeNumber', 'averageRating']]
            df2 = df2[['seasonNumber', 'episodeNumber', 'numVotes']]
            # result_matrix = transform_to_heatmap(df1, df2)
            # result_metrics = imdb_series_metrics(df)
            # serialize_json(result_matrix, result_metrics, title)

            result_matrix = transform_to_heatmap(df1, df2)
            result_matrix2 = imdb_series_metrics(df, tconst, title)
            serialize_json(result_matrix, result_matrix2, title)
            # with open(f"./heatmap_parsed/HEATMAP_{SERIES_QUERY.replace(' ', '_')}.json", "w") as f:
            #     json.dump(result_matrix, f, indent=4, ensure_ascii=False)
            # with open(f"./heatmap_parsed/HEATMAP_{SERIES_QUERY.replace(' ', '_')}_METRICS.json", "w") as f:
            #     json.dump(result_metrics, f, indent=4, ensure_ascii=False)
            print(datetime.today(), " : ", title, " finished parsing...")
        except():
            print(title, " failed to parse:::")

    # print(datetime.now() - START)
    print(datetime.today(), " : ", "ALL SERIES DONE PARSING.")
    print(datetime.now() - START_ZERO)
