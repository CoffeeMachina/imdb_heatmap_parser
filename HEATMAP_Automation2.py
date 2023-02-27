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
                    "y": v[2]
                }
            ]
        })
    for episode_index, v in df_votes.iterrows():
        heatmap_container2.append({
            "id": "S" + str(int(v[0])),
            "data": [
                {
                    "x": "E" + str(int(v[1])),
                    "y": v[2]
                }
            ]
        })

    return heatmap_container, heatmap_container2


def imdb_series_metrics(dataframe, tconst, series_name):
    return {
        'tconst': tconst,
        'name': series_name,
        'average_rating': dataframe['averageRating'].mean(),
        'total_hours': round(dataframe['runtimeMinutes'].sum() / 60, 2),
        'min_score': dataframe['averageRating'].min(),
        'max_score': dataframe['averageRating'].max(),
        'std_rating': dataframe['averageRating'].std()
    }


def serialize_json(result_matrix, result_metrics):
    with open(f"./heatmap_parsed/HEATMAP_{SERIES_QUERY.replace(' ', '_')}_METRICS.json", "w") as f:
        json.dump(
            {
                "ratings": result_matrix[0],
                "votes": result_matrix[1],
                "metrics": result_metrics
            }

            , f, indent=4, ensure_ascii=False)

    # with open(f"./heatmap_parsed/HEATMAP_{SERIES_QUERY.replace(' ', '_')}.json", "w") as f:
    #     json.dump(result_matrix, f, indent=4, ensure_ascii=False)
    # with open(f"./heatmap_parsed/HEATMAP_{SERIES_QUERY.replace(' ', '_')}_METRICS.json", "w") as f:
    #     json.dump(result_metrics, f, indent=4, ensure_ascii=False)
    # print(datetime.today(), " : ", series, " finished parsing...")


# p1
if __name__ == "__main__":
    START_ZERO = datetime.now()
    # SERIES_LIST = ["Dexter", "Game of Thrones", "House of the Dragon", "Boardwalk Empire", "Breaking Bad",
    #                "Chernobyl", "Mindhunter", "Fargo", "Barry", "Ozark", "Better Call Saul", "One Punch Man",
    #                "Homeland", "Ray Donovan", "House of Cards", "The Walking Dead"]

    print("SERIES TO PARSE: ", len(SERIES_LIST))
    for series in SERIES_LIST:
        try:
            print(datetime.today(), " : ", series, " started...")

            START = datetime.now()
            db_user = os.environ.get('DB_USER_imdb')
            db_pw = os.environ.get('DB_PASSWORD_imdb')
            connection = mysql.connector.connect(host="localhost", user=db_user, passwd=db_pw, database="imdb_dec_2022")

            SERIES_QUERY = series
            print(f"Searching for {SERIES_QUERY}...")

            user1 = f"""SELECT * FROM title_basics WHERE primaryTitle LIKE "{SERIES_QUERY}" and titleType = 'tvSeries';"""
            tconst = pd.read_sql_query(user1, connection)
            series_id = tconst['tconst'][0]
            q1 = "SELECT a.tconst, parentTconst, seasonNumber, episodeNumber, startYear,averageRating ,numVotes,primaryTitle, runtimeMinutes FROM title_episode a JOIN title_ratings b ON a.tconst = b.tconst JOIN title_basics c ON a.tconst = c.tconst WHERE parentTconst = '" + series_id + "'"
            # q2 = "SELECT a.tconst,a.primaryTitle, b.averageRating, b.numVotes FROM title_basics a JOIN title_ratings b ON a.tconst = b.tconst WHERE a.tconst = '" + series_id + "';"

            ga = pd.read_sql_query(q1, connection)
            # ga2 = pd.read_sql_query(q2, connection)
            ga.fillna(0, inplace=True)
            ga['runtimeMinutes'] = ga['runtimeMinutes'].astype('int')
            ga['startYear'] = ga['startYear'].astype('int64')
            df = ga.copy()
            df2 = ga.copy()
            df1 = df[['seasonNumber', 'episodeNumber', 'averageRating']]
            df2 = df2[['seasonNumber', 'episodeNumber', 'numVotes']]
            result_matrix = transform_to_heatmap(df1, df2)
            result_metrics = imdb_series_metrics(df)

            # with open(f"./heatmap_parsed/HEATMAP_{SERIES_QUERY.replace(' ', '_')}.json", "w") as f:
            #     json.dump(result_matrix, f, indent=4, ensure_ascii=False)
            # with open(f"./heatmap_parsed/HEATMAP_{SERIES_QUERY.replace(' ', '_')}_METRICS.json", "w") as f:
            #     json.dump(result_metrics, f, indent=4, ensure_ascii=False)
            # print(datetime.today(), " : ", series, " finished parsing...")
        except:
            print(series, " failed to parse")

    # print(datetime.now() - START)
    print(datetime.today(), " : ", "ALL SERIES DONE PARSING.")
    print(datetime.now() - START_ZERO)
