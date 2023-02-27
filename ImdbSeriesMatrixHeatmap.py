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


def transform_df_json(dataframe):
    """this argument takes a json shaped by records and returns a viable json for nivo heatmaps"""
    heatmap_container = []
    # df_parse = dataframe.to_json(orient="records")
    # df0 = json.loads(df_parse)
    dataframe.to_json("ZZZ_df_fix.json", orient="records")
    df0 = pd.read_json("ZZZ_df_fix.json")
    for episode, (x, y) in enumerate(df0.iterrows()):
        for season, rating in zip(y.index, y.values):
            heatmap_container.append(
                {
                    "id": f"E {episode}",
                    "data": [
                        {
                            "x": "S " + str(re.findall(r'\d{1}', season)[0]),
                            "y": rating
                        }
                    ]
                }
            )
    return heatmap_container


def imdb_series_metrics(dataframe):
    # LOWER_BOUND, FLOOR = dataframe['averageRating'].mean() - 3 * dataframe['averageRating'].std(), 1
    # MIN_SCORE = FLOOR if LOWER_BOUND < FLOOR else LOWER_BOUND
    # UPPER_BOUND, CAP = dataframe['averageRating'].mean() + 3 * dataframe['averageRating'].std(), 10
    # MAX_SCORE = CAP if UPPER_BOUND > CAP else UPPER_BOUND

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
    q2 = "SELECT a.tconst,a.primaryTitle, b.averageRating, b.numVotes FROM title_basics a JOIN title_ratings b ON a.tconst = b.tconst WHERE a.tconst = '" + series_id + "';"
    ga = pd.read_sql_query(q1, connection)
    ga.fillna(0, inplace=True)
    ga2 = pd.read_sql_query(q2, connection)
    title = ga2['primaryTitle'][0]
    ga['runtimeMinutes'] = ga['runtimeMinutes'].astype('int')
    ga['startYear'] = ga['startYear'].astype('int64')
    df = ga.copy()
    df1 = df[['seasonNumber', 'episodeNumber', 'averageRating']].copy()
    p1 = df1.pivot('episodeNumber', 'seasonNumber')
    # p1.to_json(r"./data/got_ratings_records.json", orient='records')  # MATRIX PRE-PROCESS
    result_matrix = transform_df_json(p1)
    with open(f"./heatmap_parsed/HEATMAP_{SERIES_QUERY}.json", "w") as f:
        json.dump(result_matrix, f, indent=4, ensure_ascii=False)
    result_metrics = imdb_series_metrics(df)
    with open(f"./heatmap_parsed/HEATMAP_{SERIES_QUERY}_METRICS.json", "w") as f:
        json.dump(result_metrics, f, indent=4, ensure_ascii=False)
    print(datetime.now() - START)
