import pymongo
import threading
import traceback
from utils import battlenet
from utils import redis
from utils import datetime
from utils import config
from utils import keys
from datetime import timedelta


mongo_client = pymongo.MongoClient("mongodb://***REMOVED***:***REMOVED***@***REMOVED***:***REMOVED***")
mongo_db = mongo_client.yf_sczone
task_size = 10


def update_ladder(ladder, character):
    now = datetime.current_time()
    if "createTime" not in ladder:
        ladder["createTime"] = now
    ladder["updateTime"] = now
    ladder["active"] = True
    mongo_db.ladders.update_one({"code": ladder["code"]}, {"$set": ladder}, upsert=True)
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

        for team_member in ladder_team["teamMembers"]:
            now = datetime.current_time()
            mongo_db.characters.update_one(
                {"code": team_member["code"]},
                {
                    "$set": {
                        "code": team_member["code"],
                        "regionNo": team_member["regionNo"],
                        "realmNo": team_member["realmNo"],
                        "profileNo": team_member["profileNo"],
                        "displayName": team_member["displayName"],
                        "clanTag": team_member["clanTag"],
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
                print(f"ladder updated: {ladder['code']}")
                return True
    return False


def character_task(region_no):
    try:
        task_index = redis.incr(keys.character_task_current_no(region_no))
        skip = (task_index - 1) * task_size
        characters = list(mongo_db.characters.find({"regionNo": region_no}).sort("code").skip(skip).limit(task_size))
        if redis.setnx(keys.character_task_start_time(region_no), datetime.current_time_str()):
            print(f"character task start")
        else:
            print(f"character task continue, skip: {skip}, limit:{task_size}")
        if len(characters) == 0:
            # 执行完成
            task_start_time = redis.get(keys.character_task_start_time(region_no))
            task_duration_seconds = datetime.get_duration_seconds(task_start_time, datetime.current_time_str())
            print(f"character task done, duration: {task_duration_seconds}s")
            redis.set(f"stats:duration:task:character:{datetime.current_time_str_short()}", task_duration_seconds)
            redis.delete(keys.character_task_current_no(region_no))
            redis.delete(keys.character_task_start_time(region_no))
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
        character_task(region_no)
    except Exception:
        print(traceback.format_exc())
        # 出错后，延迟 5 秒递归，防止过快重试
        threading.Timer(5, character_task, args=(region_no,)).start()


def ladder_task(region_no):
    try:
        task_index = redis.incr(keys.ladder_task_current_no(region_no))
        skip = (task_index - 1) * task_size
        ladders = list(
            mongo_db.ladders.find({"regionNo": region_no, "active": True}).sort("code").skip(skip).limit(task_size)
        )
        if redis.setnx(keys.ladder_task_start_time(region_no), datetime.current_time_str()):
            print(f"ladder task start")
        else:
            print(f"ladder task continue, skip: {skip}, limit:{task_size}")
        if len(ladders) == 0:
            # 执行完成
            task_start_time = redis.get(keys.ladder_task_start_time(region_no))
            task_duration_seconds = datetime.get_duration_seconds(task_start_time, datetime.current_time_str())
            print(f"ladder task done, duration: {task_duration_seconds}s")
            redis.set(f"stats:duration:task:ladder:{datetime.current_time_str_short()}", task_duration_seconds)

            # TODO: 将 team 更新时间早于 ladder job startTime - 3 天 的置为非活跃
            redis.delete(keys.ladder_task_current_no(region_no))
            redis.delete(keys.ladder_task_start_time(region_no))

            # 任务完成后，休息 1 分钟继续
            threading.Timer(60, ladder_task, args=(region_no,)).start()
        else:
            for ladder in ladders:
                if redis.lock(f"ladder:{ladder['code']}", timedelta(minutes=10)):
                    if not get_ladder_active_status(ladder):
                        mongo_db.ladders.update_one(
                            {"code": ladder["code"]}, {"$set": {"active": False, "updateTime": datetime.current_time()}}
                        )
                        redis.unlock(f"ladder:{ladder['code']}")
                        redis.delete(f"ladder:active:{ladder['code']}")

            # 递归调用
            ladder_task(region_no)
    except Exception:
        print(traceback.format_exc())
        # 出错后，延迟 5 秒递归，防止过快重试
        threading.Timer(5, ladder_task, args=(region_no,)).start()


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

    # 初始化数据
    # update_ladder(
    #     {"code": "1_303247", "number": 303247, "regionNo": 1, "league": "platinum", "gameMode": "1v1"},
    #     {"regionNo": 1, "realmNo": 1, "profileNo": 11345205},
    # )

    # update_ladder(
    #     {"code": "2_239600", "number": 239600, "regionNo": 2, "league": "grandmaster", "gameMode": "1v1"},
    #     {"regionNo": 2, "realmNo": 1, "profileNo": 3437681},
    # )

    # update_ladder(
    #     {"code": "3_76612", "number": 76612, "regionNo": 3, "league": "platinum", "gameMode": "1v1"},
    #     {"regionNo": 3, "realmNo": 1, "profileNo": 756147},
    # )

    # update_ladder(
    #     {"code": "5_63504", "number": 63504, "regionNo": 5, "league": "master", "gameMode": "1v1"},
    #     {"regionNo": 5, "realmNo": 1, "profileNo": 526043},
    # )

    # 启动角色轮询任务
    for _ in range(config.getint("app", "characterJobThreads")):
        threading.Thread(target=character_task, args=(1,)).start()
        threading.Thread(target=character_task, args=(2,)).start()
        threading.Thread(target=character_task, args=(3,)).start()
        threading.Thread(target=character_task, args=(5,)).start()

    # 启动天梯轮询任务
    for _ in range(config.getint("app", "ladderJobThreads")):
        threading.Thread(target=ladder_task, args=(1,)).start()
        threading.Thread(target=ladder_task, args=(2,)).start()
        threading.Thread(target=ladder_task, args=(3,)).start()
        threading.Thread(target=ladder_task, args=(5,)).start()
