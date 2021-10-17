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
    ladder["active"] = True
    mongo_db.ladders.update_one({"code": ladder["code"]}, {"$set": ladder})
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

            # TODO: api 更新 TeamCharacter

    return True


def get_ladder_active_status(ladder):
    teams = mongo_db.teams.find({"ladderCode": ladder["code"]})
    for team in teams:
        for teamMember in team["teamMembers"]:
            if update_ladder(ladder, teamMember):
                return True
    return False


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
        else:
            for character in characters:
                character_all_ladders = battlenet.get_character_all_ladders(
                    character["regionNo"], character["realmNo"], character["profileNo"]
                )

                for ladder in character_all_ladders:
                    if redis.get(f"ladder:active:{ladder['code']}") is None:
                        # TODO: api update ladder
                        redis.set(f"ladder:active:{ladder['code']}", "1")
                        if redis.lock(f"ladder:{ladder['code']}", timedelta(minutes=30)):
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
    try:
        task_index = redis.incr(constants.LADDER_TASK_CURRENT_NO)
        skip = (task_index - 1) * task_size
        ladders = list(mongo_db.ladders.find({"active": True}).sort("code").skip(skip).limit(task_size))
        if redis.setnx(constants.LADDER_TASK_START_TIME, datetime.current_time_str()):
            print(f"ladder task start")
        else:
            print(f"ladder task continue, skip: {skip}, limit:{task_size}")
        if len(ladders) == 0:
            # 执行完成
            task_start_time = redis.get(constants.LADDER_TASK_START_TIME)
            task_duration_seconds = datetime.get_duration_seconds(task_start_time, datetime.current_time_str())
            print(f"ladder task done, duration: {task_duration_seconds}s")

            # TODO: 将 team 更新时间早于 ladder job startTime - 3 天 的置为非活跃

            redis.delete(constants.LADDER_TASK_CURRENT_NO)
            redis.delete(constants.LADDER_TASK_START_TIME)
        else:
            for ladder in ladders:
                if redis.lock(f"ladder:{ladder['code']}", timedelta(minutes=30)):
                    if not get_ladder_active_status(ladder):
                        mongo_db.ladders.update_one(
                            {"code": ladder["code"]}, {"$set": {"active": False, "updateTime": datetime.current_time()}}
                        )
                        redis.unlock(f"ladder:{ladder['code']}")
                        redis.delete(f"ladder:active:{ladder['code']}")

        # 递归调用
        ladder_task()
    except Exception:
        print(traceback.format_exc())
        # 出错后，延迟 5 秒递归，防止过快重试
        threading.Timer(5, ladder_task).start()


def lock_test(i):
    if redis.lock("test:1", timedelta(milliseconds=1000)):
        print(f"我获取到锁了:{i}")
    else:
        print(f"未获取到锁: {i}")


if __name__ == "__main__":
    # 创建 mongo index
    mongo_db.characters.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)
    mongo_db.teams.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)
    mongo_db.teams.create_index([("ladderCode", pymongo.ASCENDING)], name="idx_ladderCode", background=True)
    mongo_db.ladders.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)
    mongo_db.ladders.create_index([("active", pymongo.ASCENDING)], name="idx_active", background=True)

    # 启动角色轮询任务
    # for _ in range(config.getint("app", "characterJobThreads")):
    #     threading.Thread(target=character_task).start()
    # character_task()

    # 启动天梯轮询任务
    # for _ in range(config.getint("app", "ladderJobThreads")):
    #     threading.Thread(target=ladder_task).start()

    # mongo_db.ladders.update_many({}, {"$set": {"active": True}})
    # mongo_db.teams.update_many(
    #     {"teamMembers.region": 5},
    #     {
    #         "$set": {
    #             "teamMembers.$[elem].regionNo": "elem.region",
    #             "teamMembers.$[elem].realmNo": "elem.realm",
    #             "teamMembers.$[elem].profileNo": "elem.profile",
    #         },
    #     },
    # )
    start_time = datetime.current_time_str()
    list = mongo_db.teams.find({"teamMembers.code": {"$exists": False}}).limit(100000)
    print(f"获取数据: {datetime.get_duration_seconds(start_time, datetime.current_time_str())}s")
    for item in list:
        team_members = item["teamMembers"]
        new_team_members = []
        for team_member in team_members:
            new_team_members.append(
                {
                    "code": f"{team_member['region']}_{team_member['realm']}_{team_member['id']}",
                    "regionNo": team_member["region"],
                    "realmNo": team_member["realm"],
                    "profileNo": team_member["id"],
                    "displayName": team_member["displayName"],
                    "clanTag": team_member["clanTag"] if "clanTag" in team_member else None,
                }
            )
        mongo_db.teams.update_one(
            {"code": item["code"]},
            {"$set": {"teamMembers": new_team_members}},
        )
    print(f"累计时间: {datetime.get_duration_seconds(start_time, datetime.current_time_str())}s")
