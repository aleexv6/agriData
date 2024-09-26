import config
from pymongo import MongoClient

def get_database():
    CONNECTION_STRING = "mongodb://"+config.USER+":"+config.PASS+"@146.59.157.18:27017/admin"
    client = MongoClient(CONNECTION_STRING)
    return client['cot_data']

def get_database_price():
    CONNECTION_STRING = "mongodb://"+config.USER+":"+config.PASS+"@146.59.157.18:27017/admin"
    client = MongoClient(CONNECTION_STRING)
    return client['agri_data']