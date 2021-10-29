import pymongo
from utils import config

mongo_client = pymongo.MongoClient(
    f"mongodb://{config.mongo['user']}:{config.mongo['password']}@{config.mongo['host']}:{config.mongo['port']}"
)
mongo = mongo_client.yf_sczone
