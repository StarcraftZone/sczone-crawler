from utils.mongo import mongo
from utils import datetime


def insert(region_no, type, data):
    mongo.stats.insert_one(
        {
            "regionNo": region_no,
            "date": datetime.current_date_str(),
            "type": type,
            "time": datetime.current_time(),
            "data": data,
        }
    )


def incr(region_no, type, data):
    mongo.stats.update_one(
        {
            "regionNo": region_no,
            "type": type,
            "date": datetime.current_date_str(),
        },
        {"$inc": data},
        upsert=True,
    )
