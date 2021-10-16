from helpers import bnet_helper
from helpers import redis_helper
import pymongo
from datetime import datetime
import threading
import traceback

mongo_client = pymongo.MongoClient("mongodb://***REMOVED***:***REMOVED***@***REMOVED***:***REMOVED***")
mongo_db = mongo_client.yf_sczone
task_size = 10


def update_ladder(ladder, character):
    now = datetime.now()
    ladder["updateTime"] = now
    mongo_db.ladders.update_one(
        {"code": ladder["code"]}, {"$set": ladder, "$setOnInsert": {"createTime": now}}, upsert=True
    )
    # TODO: api 更新 ladder

    ladder_all_teams = bnet_helper.get_ladder_all_teams(
        character["regionNo"], character["realmNo"], character["profileNo"], ladder
    )
    if len(ladder_all_teams) == 0:
        return False
    for ladder_team in ladder_all_teams:
        now = datetime.now()
        ladder_team["updateTime"] = now
        mongo_db.teams.update_one(
            {"code": ladder_team["code"]}, {"$set": ladder_team, "$setOnInsert": {"createTime": now}}, upsert=True
        )
        # TODO: api 更新 team, "membersData": json.dumps(team["teamMembers"]),
        # TODO: 更新 mmr redis
        print(f"update ladder_team: {ladder_team['code']}")
        for team_member in ladder_team["teamMembers"]:
            now = datetime.now()
            character_code = f"{team_member['region']}_{team_member['realm']}_{team_member['id']}"
            mongo_db.characters.update_one(
                {"code": character_code},
                {
                    "$set": {
                        "code": character_code,
                        "regionNo": team_member["region"],
                        "realmNo": team_member["realm"],
                        "profileNo": team_member["id"],
                        "displayName": team_member["displayName"],
                        "clanTag": team_member["clanTag"] if "clanTag" in team_member else None,
                        "updateTime": now,
                    },
                    "$setOnInsert": {"createTime": now},
                },
                upsert=True,
            )
            # TODO: api 更新 Character

            team_member_code = f"{ladder_team['code']}_{character_code}"
            mongo_db.teamMembers.update_one(
                {"code": team_member_code},
                {
                    "$set": {
                        "code": team_member_code,
                        "teamCode": ladder_team["code"],
                        "characterCode": character_code,
                        "favoriteRace": team_member["favoriteRace"] if "favoriteRace" in team_member else None,
                        "updateTime": now,
                    },
                    "$setOnInsert": {"createTime": now},
                },
                upsert=True,
            )
            # TODO: api 更新 TeamCharacter

    return True


def character_job():
    for i in range(20):
        threading.Thread(target=character_task).start()


def character_task():
    try:
        task_index = redis_helper.redis_connection.incr("sczone-crawler:task:character:current")
        skip = (task_index - 1) * task_size
        characters = list(mongo_db.characters.find().sort("code").skip(skip).limit(task_size))
        if len(characters) == 0:
            # TODO, 执行完成
            print("done")
            redis_helper.redis_connection.delete("sczone-crawler:task:character:current")
        for character in characters:
            character_all_ladders = bnet_helper.get_character_all_ladders(
                character["regionNo"], character["realmNo"], character["profileNo"]
            )
            for ladder in character_all_ladders:
                ladder_active_redis_key = f"sczone-crawler:ladder:active:{ladder['code']}"
                ladder_active = redis_helper.redis_connection.get(ladder_active_redis_key)
                if ladder_active is None or ladder_active == "0":
                    # TODO: api update ladder
                    redis_helper.redis_connection.set(ladder_active_redis_key, "1")
                    print(f"update_ladder: {ladder['code']}")
                    update_ladder(ladder, character)
                else:
                    print(f"跳过更新 ladder: {ladder['code']}")
        character_task()
    except Exception:
        print(traceback.format_exc())
        threading.Timer(5, character_task).start()


if __name__ == "__main__":
    # print(bnet_helper.get_character_all_ladders(5, 1, 526043))
    # update_ladder(
    #     {"code": "5_63504", "number": 63504, "regionNo": 5, "league": "master", "gameMode": "1v1"},
    #     {"regionNo": 5, "realmNo": 1, "profileNo": 526043},
    # )
    # character_job()
    mongo_db.characters.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)
    mongo_db.teams.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)
    mongo_db.ladders.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)
    mongo_db.teamMembers.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)

    character_task()
