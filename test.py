from utils import api, json, redis, mongo, battlenet, log
from pymongo import UpdateOne
from bson import json_util
from threading import Thread, Lock
import time
from tenacity import retry, wait_fixed, stop_after_attempt, RetryError

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

# team_codes_to_inactive = []
# for region_no in [1, 2, 3, 5]:
#     for game_mode in [
#         "1v1",
#         "2v2",
#         "3v3",
#         "4v4",
#         "2v2_random",
#         "3v3_random",
#         "4v4_random",
#         "archon",
#     ]:
#         teams = mongo.mongo.teams.find({"active": 0, "regionNo": region_no, "gameMode": game_mode}).limit(1000000)
#         for team in teams:
#             team_codes_to_inactive.append(team["code"])

# print(len(team_codes_to_inactive))
# api.post(f"/team/batch", {"regionNo": region_no, "gameMode": game_mode, "codes": team_codes_to_inactive})


# teams = mongo.mongo.teams.find({}).limit(1000000)
# for team in teams:
#     if team["code"] != battlenet.get_team_code(team["regionNo"], team["gameMode"], team["teamMembers"]):
#         mongo.mongo.teams.delete_one({"code": team["code"]})


# index = 0
# lock = Lock()


# def test(i):
#     global index
#     with lock:
#         index = index + 1
#         print(i, index)
#     if index < 50:
#         time.sleep(0.1)
#         test(i)


# for i in range(10):
#     Thread(target=test, args=(i,)).start()


# season_info = battlenet.get_season_info(5)
# print(season_info)

# response = api.get("/season/48")

# print(response)
log.debug(0, "d Hello, World!")
log.info(0, "i Hello, World!")
log.warn(0, "w Hello, World!")
log.error(0, "e Hello, World!")
