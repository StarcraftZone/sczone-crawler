from utils import api, json, redis, mongo
from pymongo import UpdateOne

# data = api.get_api_response("/ladder/1")
# print(json.dumps(data))
test = redis.getset("test:x", 1)
print(test)
operations = [
    UpdateOne({"code": 1}, {"$set": {"gg": 2}}, upsert=True),
    UpdateOne({"code": 2}, {"$set": {"gg": 3}}, upsert=True),
    UpdateOne({"code": 3}, {"$set": {"gg": 4}}, upsert=True),
    UpdateOne({"code": 4}, {"$set": {"gg": 5}}, upsert=True),
]
mongo.mongo.test.bulk_write(operations)
