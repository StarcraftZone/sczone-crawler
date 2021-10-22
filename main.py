import threading
import time
import traceback
from datetime import timedelta

import pymongo
from pymongo import UpdateOne

from utils import api, battlenet, datetime, keys, log, redis, stats
from utils.mongo import mongo


def inactive_ladder(region_no, ladder_no):
    ladder_active_status = redis.getset(f"status:region:{region_no}:ladder:{ladder_no}:active", 0)
    if ladder_active_status != "0":
        update_result = mongo.ladders.update_one(
            {"code": f"{region_no}_{ladder_no}"},
            {"$set": {"active": 0, "updateTime": datetime.current_time()}},
        )
        if update_result.modified_count > 0:
            log.info(f"({region_no}) inactive ladder: {ladder_no}")


def update_ladder(ladder_no, character):
    ladder, teams = battlenet.get_ladder_and_teams(
        character["regionNo"], character["realmNo"], character["profileNo"], ladder_no
    )

    if ladder is None or len(teams) == 0:
        return False

    ladder_active_status = redis.getset(f"status:region:{ladder['regionNo']}:ladder:{ladder['code']}:active", 1)
    if ladder_active_status != "1":
        now = datetime.current_time()
        ladder["active"] = 1
        ladder["updateTime"] = now
        update_result = mongo.ladders.update_one(
            {"code": ladder["code"]}, {"$set": ladder, "$setOnInsert": {"createTime": now}}, upsert=True
        )
        if update_result.upserted_id is not None:
            log.info(f"({character['regionNo']}) found new ladder: {ladder['code']}")

    bulk_operations = []
    for team in teams:
        now = datetime.current_time()
        team["active"] = 1
        team["updateTime"] = now
        bulk_operations.append(
            UpdateOne({"code": team["code"]}, {"$set": team, "$setOnInsert": {"createTime": now}}, upsert=True)
        )
    if len(bulk_operations) > 0:
        mongo.teams.bulk_write(bulk_operations)
        api.post(f"/team/batch", teams)

    return True


def ladder_task(region_no):
    while True:
        try:
            min_active_ladder_no = (
                mongo.ladders.find({"regionNo": region_no, "active": 1}).sort("number", 1).limit(1)[0]["number"]
            )

            max_active_ladder_no = (
                mongo.ladders.find({"regionNo": region_no, "active": 1})
                .sort("number", pymongo.DESCENDING)
                .limit(1)[0]["number"]
            )
            if redis.setnx(keys.ladder_task_start_time(region_no), datetime.current_time_str()):
                log.info(f"({region_no}) ladder task start")
            if redis.setnx(keys.ladder_task_current_no(region_no), min_active_ladder_no):
                current_ladder_no = min_active_ladder_no
            else:
                current_ladder_no = redis.incr(keys.ladder_task_current_no(region_no))

            ladder_members = battlenet.get_ladder_members(region_no, current_ladder_no)
            if len(ladder_members) == 0:
                # 成员为空，将 ladder 置为非活跃
                inactive_ladder(region_no, current_ladder_no)

                # 最大 ladder 编号再往后跑 10 个，都不存在则认为任务完成
                if current_ladder_no > max_active_ladder_no + 10:
                    if redis.lock(keys.ladder_task_done(region_no), timedelta(minutes=1)):
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

                        # 将当前 region 中 team 更新时间早于 ladder job startTime - 1 天且活跃的 team 置为非活跃
                        task_start_time = datetime.get_time(redis.get(keys.ladder_task_start_time(region_no)))
                        teams_to_inactive = mongo.teams.find(
                            {
                                "regionNo": region_no,
                                "updateTime": {"$lte": datetime.minus(task_start_time, timedelta(days=1))},
                                "$or": [{"active": 1}, {"active": None}],
                            }
                        )
                        teams_to_inactive_obj = []

                        bulk_operations = []
                        for team_to_inactive in teams_to_inactive:
                            log.info(f"({region_no}) inactive team: {team_to_inactive['code']}")
                            bulk_operations.append(
                                UpdateOne(
                                    {"code": team_to_inactive["code"]},
                                    {"$set": {"active": 0, "updateTime": datetime.current_time()}},
                                )
                            )
                            teams_to_inactive_obj.append(
                                {
                                    "code": team_to_inactive["code"],
                                    "active": 0,
                                    "ladderCode": team_to_inactive["ladderCode"],
                                    "regionNo": team_to_inactive["regionNo"],
                                    "gameMode": team_to_inactive["gameMode"],
                                    "league": team_to_inactive["league"],
                                    "points": team_to_inactive["points"],
                                    "wins": team_to_inactive["wins"],
                                    "losses": team_to_inactive["losses"],
                                    "total": team_to_inactive["total"],
                                    "winRate": team_to_inactive["winRate"],
                                    "mmr": team_to_inactive["mmr"],
                                    "joinLadderTime": team_to_inactive["joinLadderTime"],
                                    "teamMembers": team_to_inactive["teamMembers"],
                                }
                            )
                        if len(bulk_operations) > 0:
                            mongo.teams.bulk_write(bulk_operations)
                            api.post(f"/team/batch", teams_to_inactive_obj)

                        redis.delete(keys.ladder_task_current_no(region_no))
                        redis.delete(keys.ladder_task_start_time(region_no))
            else:
                # 更新 Character
                ladder_updated = False
                ladder_update_retry_times = 0
                bulk_operations = []
                for ladder_member in ladder_members:
                    now = datetime.current_time()
                    bulk_operations.append(
                        UpdateOne(
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
                    )

                    if not ladder_updated and ladder_update_retry_times < 10:
                        # 为提升速度，只重试 10 次
                        ladder_updated = update_ladder(current_ladder_no, ladder_member)
                        ladder_update_retry_times += 1
                if len(bulk_operations) > 0:
                    mongo.characters.bulk_write(bulk_operations)
                    api.post("/character/batch", ladder_members)

                if not ladder_updated:
                    # 通过新方法未能获取到 ladder 信息
                    inactive_ladder(region_no, current_ladder_no)
                    print(f"({region_no}) legacy ladder info problem: {current_ladder_no}")

        except:
            log.error(traceback.format_exc())
            # 出错后，休眠 1 分钟
            time.sleep(60)


if __name__ == "__main__":
    # 创建 mongo index
    mongo.characters.create_index([("code", 1)], name="idx_code", unique=True, background=True)
    mongo.characters.create_index([("regionNo", 1)], name="idx_regionNo", background=True)
    mongo.teams.create_index([("code", 1)], name="idx_code", unique=True, background=True)
    mongo.teams.create_index([("ladderCode", 1)], name="idx_ladderCode", background=True)
    mongo.teams.create_index([("active", 1)], name="idx_active", background=True)
    mongo.teams.create_index(
        [("regionNo", 1), ("active", 1), ("updateTime", 1)],
        name="idx_inactive",
        background=True,
    )
    mongo.ladders.create_index([("code", 1)], name="idx_code", unique=True, background=True)
    mongo.ladders.create_index([("active", 1)], name="idx_active", background=True)
    mongo.ladders.create_index([("regionNo", 1)], name="idx_regionNo", background=True)
    mongo.stats.create_index([("regionNo", 1)], name="idx_regionNo", background=True)
    mongo.stats.create_index([("date", 1)], name="idx_date", background=True)
    mongo.stats.create_index([("type", 1)], name="idx_type", background=True)

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
