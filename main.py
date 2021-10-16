import pymongo
import threading
import traceback
from utils import battlenet
from utils import redis
from utils import datetime
from utils import constants
from utils import config
from datetime import timedelta
import time


mongo_client = pymongo.MongoClient("mongodb://***REMOVED***:***REMOVED***@***REMOVED***:***REMOVED***")
mongo_db = mongo_client.yf_sczone
task_size = 10


def update_ladder(ladder, character):
    now = datetime.current_time()
    ladder["updateTime"] = now
    mongo_db.ladders.update_one(
        {"code": ladder["code"]}, {"$set": ladder, "$setOnInsert": {"createTime": now}}, upsert=True
    )
    # TODO: api 更新 ladder

    ladder_all_teams = battlenet.get_ladder_all_teams(
        character["regionNo"], character["realmNo"], character["profileNo"], ladder
    )
    if len(ladder_all_teams) == 0:
        return False
    for ladder_team in ladder_all_teams:
        now = datetime.current_time()
        ladder_team["updateTime"] = now
        mongo_db.teams.update_one(
            {"code": ladder_team["code"]}, {"$set": ladder_team, "$setOnInsert": {"createTime": now}}, upsert=True
        )
        # TODO: api 更新 team, "membersData": json.dumps(team["teamMembers"]),
        # TODO: 更新 mmr redis
        print(f"update ladder_team: {ladder_team['code']}")
        for team_member in ladder_team["teamMembers"]:
            now = datetime.current_time()
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


def character_task():
    try:
        task_index = redis.incr(constants.CHARACTER_TASK_CURRENT_NO)
        skip = (task_index - 1) * task_size
        characters = list(mongo_db.characters.find().sort("code").skip(skip).limit(task_size))
        if redis.setnx(constants.CHARACTER_TASK_START_TIME, datetime.current_time_str()):
            print(f"character task start")
        else:
            print(f"character task continue, skip: {skip}, limit:{task_size}")
        if len(characters) == 0:
            # 执行完成
            task_start_time = redis.get(constants.CHARACTER_TASK_START_TIME)
            task_duration_seconds = datetime.get_duration_seconds(task_start_time, datetime.current_time_str())
            print(f"character task done, duration: {task_duration_seconds}s")
            redis.delete(constants.CHARACTER_TASK_CURRENT_NO)
            redis.delete(constants.CHARACTER_TASK_START_TIME)
        for character in characters:
            character_all_ladders = battlenet.get_character_all_ladders(
                character["regionNo"], character["realmNo"], character["profileNo"]
            )

            for ladder in character_all_ladders:
                ladder_active_redis_key = f"ladder:active:{ladder['code']}"
                ladder_active = redis.get(ladder_active_redis_key)
                if ladder_active is None or ladder_active == "0":
                    # TODO: api update ladder
                    redis.set(ladder_active_redis_key, "1")
                    if redis.lock(f"ladder", timedelta(minutes=30)):
                        print(f"update_ladder: {ladder['code']}")
                        update_ladder(ladder, character)
                    else:
                        print(f"skip update_ladder: {ladder['code']}")
        # 递归调用
        character_task()
    except Exception:
        print(traceback.format_exc())
        # 出错后，延迟 5 秒递归，防止过快重试
        threading.Timer(5, character_task).start()


def ladder_task():
    print("todo")


def lock_test(i):
    if redis.lock("lock:test:1", timedelta(milliseconds=1000)):
        print(f"我获取到锁了:{i}")
    else:
        print(f"未获取到锁: {i}")


if __name__ == "__main__":
    # print(battlenet.get_character_all_ladders(5, 1, 526043))
    # update_ladder(
    #     {"code": "5_63504", "number": 63504, "regionNo": 5, "league": "master", "gameMode": "1v1"},
    #     {"regionNo": 5, "realmNo": 1, "profileNo": 526043},
    # )

    # 测试 lock
    # for i in range(10):
    #     threading.Thread(target=lock_test, args=(i,)).start()

    # 创建 mongo index
    mongo_db.characters.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)
    mongo_db.teams.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)
    mongo_db.ladders.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)
    mongo_db.teamMembers.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)

    # 启动角色轮询任务
    for _ in range(config.getint("app", "characterJobThreads")):
        threading.Thread(target=character_task).start()
    character_task()

    # 启动天梯轮询任务
    # for _ in range(config.getint("app", "ladderJobThreads")):
    #     threading.Thread(target=ladder_task).start()


