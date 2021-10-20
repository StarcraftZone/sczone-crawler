from datetime import timedelta
import threading
import traceback
import time

import pymongo

from utils import battlenet, datetime, keys, log, redis, stats, api
from utils.mongo import mongo


def update_ladder(ladder_no, character):
    ladder, teams = battlenet.get_ladder_and_teams(
        character["regionNo"], character["realmNo"], character["profileNo"], ladder_no
    )

    if ladder is None or len(teams) == 0:
        return False

    now = datetime.current_time()
    ladder["updateTime"] = now
    ladder["active"] = 1
    update_result = mongo.ladders.update_one(
        {"code": ladder["code"]}, {"$set": ladder, "$setOnInsert": {"createTime": now}}, upsert=True
    )
    if update_result.upserted_id is not None:
        log.info(f"({character['regionNo']}) found new ladder: {ladder['code']}")
        api.post("/ladder", ladder)
    else:
        api.put(f"/ladder/code/{ladder['code']}", ladder)

    for team in teams:
        now = datetime.current_time()
        team["updateTime"] = now
        mongo.teams.update_one({"code": team["code"]}, {"$set": team, "$setOnInsert": {"createTime": now}}, upsert=True)
        # TODO: api 更新 team, "membersData": json.dumps(team["teamMembers"]),
        # TODO: 更新 mmr redis
        # TODO: api 更新 TeamCharacter

    return True


def ladder_task(region_no):
    while True:
        try:
            min_active_ladder_no = (
                mongo.ladders.find({"regionNo": region_no, "active": 1})
                .sort("number", pymongo.ASCENDING)
                .limit(1)[0]["number"]
            )

            max_active_ladder_no = (
                mongo.ladders.find({"regionNo": region_no, "active": 1})
                .sort("number", pymongo.DESCENDING)
                .limit(1)[0]["number"]
            )
            if redis.setnx(keys.ladder_task_start_time(region_no), datetime.current_time_str()):
                log.info(f"({region_no}) ladder task start")
            if redis.setnx(keys.ladder_task_current_no(region_no), min_active_ladder_no - 10):
                current_ladder_no = min_active_ladder_no - 10
            else:
                current_ladder_no = redis.incr(keys.ladder_task_current_no(region_no))

            ladder_members = battlenet.get_ladder_members(region_no, current_ladder_no)
            if len(ladder_members) == 0:
                # 成员为空，将 ladder 置为非活跃
                update_result = mongo.ladders.update_one(
                    {"code": f"{region_no}_{current_ladder_no}"},
                    {"$set": {"active": 0, "updateTime": datetime.current_time()}},
                )
                if update_result.modified_count > 0:
                    log.info(f"({region_no}) inactive ladder: {current_ladder_no}")
                    api.patch(f"/ladder/code/{region_no}_{current_ladder_no}", {"active": 0})

                # 最大 ladder 编号再往后跑 10 个，都不存在则认为任务完成
                if current_ladder_no > max_active_ladder_no + 10:
                    if redis.lock(keys.ladder_task_done(region_no), timedelta(seconds=10)):
                        task_duration_seconds = datetime.get_duration_seconds(
                            redis.get(keys.ladder_task_start_time(region_no)), datetime.current_time_str()
                        )
                        log.info(
                            f"({region_no}) ladder task done, max_ladder_no: {max_active_ladder_no}, duration: {task_duration_seconds}s"
                        )

                        stats.insert(
                            region_no,
                            "ladder_task",
                            {
                                "maxActiveLadderNo": max_active_ladder_no,
                                "duration": task_duration_seconds,
                            },
                        )

                        # TODO: 将 team 更新时间早于 ladder job startTime - 3 天 的置为非活跃
                        redis.delete(keys.ladder_task_current_no(region_no))
                        redis.delete(keys.ladder_task_start_time(region_no))
            else:
                update_ladder(current_ladder_no, ladder_members[0])

                for ladder_member in ladder_members:
                    now = datetime.current_time()
                    update_result = mongo.characters.update_one(
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
                    if update_result.upserted_id is not None:
                        log.info(f"({ladder_member['regionNo']}) found new character: {ladder_member['code']}")
                        api.post("/character", ladder_member)
                    else:
                        api.put(f"/character/code/{ladder_member['code']}", ladder_member)

        except:
            log.error(traceback.format_exc())
            # 出错后，休眠 1 分钟
            time.sleep(60)


if __name__ == "__main__":
    # 创建 mongo index
    mongo.characters.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)
    mongo.characters.create_index([("regionNo", pymongo.ASCENDING)], name="idx_regionNo", background=True)
    mongo.teams.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)
    mongo.teams.create_index([("ladderCode", pymongo.ASCENDING)], name="idx_ladderCode", background=True)
    mongo.ladders.create_index([("code", pymongo.ASCENDING)], name="idx_code", unique=True, background=True)
    mongo.ladders.create_index([("active", pymongo.ASCENDING)], name="idx_active", background=True)
    mongo.ladders.create_index([("regionNo", pymongo.ASCENDING)], name="idx_regionNo", background=True)
    mongo.stats.create_index([("regionNo", pymongo.ASCENDING)], name="idx_regionNo", background=True)
    mongo.stats.create_index([("date", pymongo.ASCENDING)], name="idx_date", background=True)
    mongo.stats.create_index([("type", pymongo.ASCENDING)], name="idx_type", background=True)

    # 遍历天梯成员任务
    for _ in range(5):
        threading.Thread(target=ladder_task, args=(1,)).start()
    for _ in range(4):
        threading.Thread(target=ladder_task, args=(2,)).start()
    for _ in range(1):
        threading.Thread(target=ladder_task, args=(3,)).start()
    for _ in range(4):
        threading.Thread(target=ladder_task, args=(5,)).start()

    log.info("sczone crawler started")
