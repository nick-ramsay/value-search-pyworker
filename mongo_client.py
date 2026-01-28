import os

from pymongo import MongoClient


def get_mongo_client():
    mongo_uri = os.getenv("MONGO_URI")
    if mongo_uri:
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            return client
        except Exception:
            pass
    return MongoClient("localhost", 27017)
