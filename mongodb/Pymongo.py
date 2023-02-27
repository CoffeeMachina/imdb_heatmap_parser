import pymongo
from pymongo import MongoClient
import os
AUTH = os.environ.get('DB_MONGODB_IMDB')
cluster = MongoClient(AUTH)
db = cluster["test"]
collection = db["imdb"]

# mongo post e.g.: {"_id", "name", "score"}
# post1 = {"name": "John Wick 2", "Score": 900}
# collection.insert_one(post1)


# query/find:

results = collection.find({"name": "John Wick 2"}).sort("score", -1).limit(1)

for r in results:
    print(r)