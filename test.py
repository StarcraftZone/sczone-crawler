from utils import api, json, redis, mongo, battlenet
from pymongo import UpdateOne
from bson import json_util

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

teams_to_inactive = []
teams = mongo.mongo.teams.find({"active": 0}).limit(1000000)
for team in teams:
    teams_to_inactive.append(
        {
            "code": team["code"],
            "active": 0,
            "ladderCode": team["ladderCode"],
            "regionNo": team["regionNo"],
            "gameMode": team["gameMode"],
            "league": team["league"],
            "points": team["points"],
            "wins": team["wins"],
            "losses": team["losses"],
            "total": team["total"],
            "winRate": team["winRate"],
            "mmr": team["mmr"],
            "joinLadderTime": team["joinLadderTime"],
            "teamMembers": team["teamMembers"],
        }
    )

print(len(teams_to_inactive))
api.post(f"/team/batch", teams_to_inactive)
