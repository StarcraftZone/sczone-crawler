import json
import threading
import traceback
from datetime import timedelta

import pymongo

from utils import battlenet, config, datetime, keys, log, redis

mongo_client = pymongo.MongoClient("mongodb://***REMOVED***:***REMOVED***@***REMOVED***:***REMOVED***")
mongo_db = mongo_client.yf_sczone
task_size = 100


def update_ladder(ladder_no, character):
    ladder, teams = battlenet.get_ladder_and_teams(
        character["regionNo"], character["realmNo"], character["profileNo"], ladder_no
    )

    if ladder is None or len(teams) == 0:
        return False

    now = datetime.current_time()
    ladder["updateTime"] = now
    ladder["active"] = True
    mongo_db.ladders.update_one(
        {"code": ladder["code"]}, {"$set": ladder, "$setOnInsert": {"createTime": now}}, upsert=True
    )
    # TODO: api 更新 ladder

    for team in teams:
        now = datetime.current_time()
        team["updateTime"] = now
        mongo_db.teams.update_one(
            {"code": team["code"]}, {"$set": team, "$setOnInsert": {"createTime": now}}, upsert=True
        )
        # TODO: api 更新 team, "membersData": json.dumps(team["teamMembers"]),
        # TODO: 更新 mmr redis
        # TODO: api 更新 TeamCharacter

    return True


def get_ladder_active_status(ladder):
    teams = mongo_db.teams.find({"ladderCode": ladder["code"]})
    for team in teams:
        for teamMember in team["teamMembers"]:
            if update_ladder(ladder, teamMember):
                return True
    return False


def character_task(region_no):
    try:
        task_index = redis.incr(keys.character_task_current_no(region_no))
        skip = (task_index - 1) * task_size
        characters = list(mongo_db.characters.find({"regionNo": region_no}).sort("code").skip(skip).limit(task_size))
        if redis.setnx(keys.character_task_start_time(region_no), datetime.current_time_str()):
            log.info(f"({region_no}) character task start")
        else:
            log.info(f"({region_no}) character task continue, skip: {skip}, limit:{task_size}")
        if len(characters) == 0:
            # 执行完成
            task_start_time = redis.get(keys.character_task_start_time(region_no))
            task_duration_seconds = datetime.get_duration_seconds(task_start_time, datetime.current_time_str())
            log.info(f"({region_no}) character task done, duration: {task_duration_seconds}s")
            redis.set(
                keys.character_task_done_stats(region_no), json.dumps({"skip": skip, "duration": task_duration_seconds})
            )
            redis.delete(keys.character_task_current_no(region_no))
            redis.delete(keys.character_task_start_time(region_no))
        else:
            for character in characters:
                character_all_ladders = battlenet.get_character_all_ladders(
                    character["regionNo"], character["realmNo"], character["profileNo"]
                )

                for ladder in character_all_ladders:
                    if redis.setnx(f"ladder:active:{ladder['code']}", datetime.current_time_str()):
                        # TODO: api update ladder
                        if redis.lock(f"ladder:{ladder['code']}", timedelta(minutes=30)):
                            update_ladder(ladder, character)
        # 递归调用
        character_task(region_no)
    except Exception:
        log.error(traceback.format_exc())
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
            log.info(f"({region_no}) ladder task start")
        else:
            log.info(f"({region_no}) ladder task continue, skip: {skip}, limit:{task_size}")
        if len(ladders) == 0:
            # 执行完成
            task_start_time = redis.get(keys.ladder_task_start_time(region_no))
            task_duration_seconds = datetime.get_duration_seconds(task_start_time, datetime.current_time_str())
            log.info(f"({region_no}) ladder task done, duration: {task_duration_seconds}s")
            redis.set(
                keys.ladder_task_done_stats(region_no), json.dumps({"skip": skip, "duration": task_duration_seconds})
            )

            # TODO: 将 team 更新时间早于 ladder job startTime - 3 天 的置为非活跃
            redis.delete(keys.ladder_task_current_no(region_no))
            redis.delete(keys.ladder_task_start_time(region_no))

            # 任务完成后，休息 1 分钟继续
            threading.Timer(60, ladder_task, args=(region_no,)).start()
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
            ladder_task(region_no)
    except Exception:
        log.error(traceback.format_exc())
        # 出错后，延迟 5 秒递归，防止过快重试
        threading.Timer(5, ladder_task, args=(region_no,)).start()


# TODO: 添加统计时间；测试方法有效性；可以干掉 character_task 和 ladder_task 了
def ladder_member_task(region_no):
    min_active_ladder_no = (
        mongo_db.ladders.find({"regionNo": region_no}).sort("number", pymongo.ASCENDING).limit(1)[0]["number"]
    )
    max_active_ladder_no = (
        mongo_db.ladders.find({"regionNo": region_no}).sort("number", pymongo.DESCENDING).limit(1)[0]["number"]
    )

    if redis.setnx(keys.ladder_member_task_start_time(region_no), datetime.current_time_str()):
        log.info(f"({region_no}) ladder_member task start")
    if redis.setnx(keys.ladder_member_task_current_no(region_no), min_active_ladder_no):
        current_ladder_no = min_active_ladder_no
    else:
        current_ladder_no = redis.incr(keys.ladder_member_task_current_no(region_no))

    log.info(
        f"({region_no}) current ladder number: {current_ladder_no}, ({min_active_ladder_no} ~ {max_active_ladder_no})"
    )

    ladder_members = battlenet.get_ladder_members(region_no, current_ladder_no)
    if len(ladder_members) == 0:
        # 成员为空，将 ladder 置为非活跃
        mongo_db.ladders.update_one(
            {"code": f"{region_no}_{current_ladder_no}"},
            {"$set": {"active": False, "updateTime": datetime.current_time()}},
        )

        # 最大 ladder 编号再往后跑 10 个，都不存在则认为任务完成
        if current_ladder_no > max_active_ladder_no + 10:
            task_start_time = redis.get(keys.ladder_member_task_start_time(region_no))
            task_duration_seconds = datetime.get_duration_seconds(task_start_time, datetime.current_time_str())
            log.info(
                f"({region_no}) ladder_member task done, max_ladder_no: {max_active_ladder_no}, duration: {task_duration_seconds}s"
            )
            redis.set(
                keys.ladder_member_task_done_stats(region_no),
                json.dumps({"max_ladder_no": max_active_ladder_no, "duration": task_duration_seconds}),
            )
            # 任务完成
            redis.delete(keys.ladder_member_task_current_no(region_no))
            redis.delete(keys.ladder_member_task_start_time(region_no))
    else:
        update_ladder(current_ladder_no, ladder_members[0])

        for ladder_member in ladder_members:
            now = datetime.current_time()
            mongo_db.characters.update_one(
                {"code": ladder_member["code"]},
                {
                    "$set": {
                        "code": ladder_member["code"],
                        "regionNo": ladder_member["regionNo"],
                        "realmNo": ladder_member["realmNo"],
                        "profileNo": ladder_member["profileNo"],
                        "displayName": ladder_member["displayName"],
                        "clanTag": ladder_member["clanTag"],
                        "clanName": ladder_member["clanName"],
                        "updateTime": now,
                    },
                    "$setOnInsert": {"createTime": now},
                },
                upsert=True,
            )
            # TODO: api 更新 Character

    ladder_member_task(region_no)


if __name__ == "__main__":
    # 创建 mongo index
    mongo_db.characters.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)
    mongo_db.characters.create_index([("regionNo", pymongo.ASCENDING)], name="idx_regionNo", background=True)
    mongo_db.teams.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)
    mongo_db.teams.create_index([("ladderCode", pymongo.ASCENDING)], name="idx_ladderCode", background=True)
    mongo_db.ladders.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)
    mongo_db.ladders.create_index([("active", pymongo.ASCENDING)], name="idx_active", background=True)
    mongo_db.ladders.create_index([("regionNo", pymongo.ASCENDING)], name="idx_regionNo", background=True)

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
    # for _ in range(config.getint("app", "characterJobThreads")):
    #     threading.Thread(target=character_task, args=(1,)).start()
    #     threading.Thread(target=character_task, args=(2,)).start()
    #     threading.Thread(target=character_task, args=(3,)).start()
    #     threading.Thread(target=character_task, args=(5,)).start()

    # 启动天梯轮询更新任务
    # for _ in range(config.getint("app", "ladderJobThreads")):
    #     threading.Thread(target=ladder_task, args=(1,)).start()
    #     threading.Thread(target=ladder_task, args=(2,)).start()
    #     threading.Thread(target=ladder_task, args=(3,)).start()
    #     threading.Thread(target=ladder_task, args=(5,)).start()

    # 遍历天梯成员任务
    threading.Thread(target=ladder_member_task, args=(1,)).start()
    threading.Thread(target=ladder_member_task, args=(2,)).start()
    threading.Thread(target=ladder_member_task, args=(3,)).start()
    threading.Thread(target=ladder_member_task, args=(5,)).start()
