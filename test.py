from utils import api, json, redis, mongo, battlenet
from pymongo import UpdateOne

# data = api.get_api_response("/ladder/1")
# print(json.dumps(data))
# test = redis.getset("test:x", 1)
# print(test)
# operations = [
#     UpdateOne({"code": 1}, {"$set": {"gg": 2}}, upsert=True),
#     UpdateOne({"code": 2}, {"$set": {"gg": 3}}, upsert=True),
#     UpdateOne({"code": 3}, {"$set": {"gg": 4}}, upsert=True),
#     UpdateOne({"code": 4}, {"$set": {"gg": 5}}, upsert=True),
# ]
# mongo.mongo.test.bulk_write(operations)

teams = mongo.mongo.teams.find().limit(1000000)
for team in teams:
    if "regionNo" not in team or team["code"] != battlenet.get_team_code(
        team["regionNo"], team["gameMode"], team["teamMembers"]
    ):
        mongo.mongo.teams.delete_one({"code": team["code"]})
        print(team["code"])
