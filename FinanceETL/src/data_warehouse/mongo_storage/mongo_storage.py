from pymongo import MongoClient
from utils import MONGO_DB_URL
from logger import setup_logger_global
logger_database = setup_logger_global(__name__, __name__ + '.log')

def get_connection_mongo():
    client = MongoClient(MONGO_DB_URL)
    return client

def create_database_and_collection(db_name, collection_name):
    client = get_connection_mongo()
    db = client[db_name]
    collection = db[collection_name]
    return collection

def insert_data(collection, data):
    try:
        result = collection.insert_one(data)
        return result
    except Exception as e:
        logger_database.error(e)

def delete_data(collection, query):
    result = collection.delete_one(query)
    if result.deleted_count > 0:
        return True
    else:
        return False

def update_data(collection, query, new_values):
    result = collection.update_one(query, {"$set": new_values})
    if result.modified_count > 0:
        return True
    else:
        return False