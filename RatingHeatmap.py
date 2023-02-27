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


def transform_to_heatmap(dataframe):
    df99 = dataframe.sort_values(by=["seasonNumber", "episodeNumber"], ascending=[True, True])
    """this argument takes a json shaped by records and returns a viable json for nivo heatmaps"""
    heatmap_container = []
    for episode_index, v in df99.iterrows():
        heatmap_container.append({
            "id": "S" + str(int(v[0])),
            "data": [
                {
                    "x": "E" + str(int(v[1])),
                    "y": v[2]
                }
            ]
        })
    return heatmap_container


def imdb_series_metrics(dataframe):
    return {
        'average_rating': dataframe['averageRating'].mean(),
        'total_hours': round(dataframe['runtimeMinutes'].sum() / 60, 2),
        'min_score': dataframe['averageRating'].min(),
        'max_score': dataframe['averageRating'].max(),
        'std_rating': dataframe['averageRating'].std()
    }


# p1
if __name__ == "__main__":
    START = datetime.now()
    db_user = os.environ.get('DB_USER_imdb')
    db_pw = os.environ.get('DB_PASSWORD_imdb')
    connection = mysql.connector.connect(host="localhost", user=db_user, passwd=db_pw, database="imdb_dec_2022")

    SERIES_QUERY = input("Please enter the series query: ")
    print(f"Searching for {SERIES_QUERY}...")

    user1 = f"""SELECT * FROM title_basics WHERE primaryTitle LIKE "{SERIES_QUERY}" and titleType = 'tvSeries';"""
    tconst = pd.read_sql_query(user1, connection)
    series_id = tconst['tconst'][0]
    q1 = "SELECT a.tconst, parentTconst, seasonNumber, episodeNumber, startYear,averageRating ,numVotes,primaryTitle, runtimeMinutes FROM title_episode a JOIN title_ratings b ON a.tconst = b.tconst JOIN title_basics c ON a.tconst = c.tconst WHERE parentTconst = '" + series_id + "'"
    ga = pd.read_sql_query(q1, connection)
    ga.fillna(0, inplace=True)
    ga['runtimeMinutes'] = ga['runtimeMinutes'].astype('int')
    ga['startYear'] = ga['startYear'].astype('int64')
    df = ga.copy()
    df1 = df[['seasonNumber', 'episodeNumber', 'averageRating']].copy()
    result_matrix = transform_to_heatmap(df1)
    with open(f"./heatmap_parsed/HEATMAP_{SERIES_QUERY.replace(' ', '_')}.json", "w") as f:
        json.dump(result_matrix, f, indent=4, ensure_ascii=False)
    result_metrics = imdb_series_metrics(df)
    with open(f"./heatmap_parsed/HEATMAP_{SERIES_QUERY.replace(' ', '_')}_METRICS.json", "w") as f:
        json.dump(result_metrics, f, indent=4, ensure_ascii=False)

    print(datetime.now() - START)
