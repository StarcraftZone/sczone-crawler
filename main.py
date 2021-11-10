from threading import Thread, Lock
import time
import traceback
from datetime import timedelta

import pymongo
from pymongo import UpdateOne

from utils import api, battlenet, datetime, keys, log, redis, stats
from utils.mongo import mongo

task_index = 0
lock = Lock()


def inactive_ladder(region_no, ladder_no):
    ladder_active_status = redis.getset(f"status:region:{region_no}:ladder:{ladder_no}:active", 0)
    if ladder_active_status != "0":
        ladder_code = f"{region_no}_{ladder_no}"
        update_result = mongo.ladders.update_one(
            {"code": ladder_code},
            {"$set": {"active": 0, "updateTime": datetime.current_time()}},
        )
        if update_result.modified_count > 0:
            log.info(f"({region_no}) inactive ladder: {ladder_no}")
    else:
        log.info(f"({region_no}) skip inactive ladder: {ladder_no}")


def inactive_teams(region_no, game_mode, teams):
    team_codes = []
    bulk_operations = []
    for team in teams:
        log.info(f"({region_no}) inactive team: {team['code']}")
        bulk_operations.append(
            UpdateOne(
                {"code": team["code"]},
                {"$set": {"active": 0}},
            )
        )
        team_codes.append(team["code"])
    if len(bulk_operations) > 0:
        mongo.teams.bulk_write(bulk_operations)
        api.post(
            f"/team/batch/inactive",
            {"regionNo": region_no, "gameMode": game_mode, "codes": team_codes},
        )


def update_ladder(ladder_no, character):
    ladder, teams = battlenet.get_ladder_and_teams(
        character["regionNo"], character["realmNo"], character["profileNo"], ladder_no
    )

    if ladder is None or len(teams) == 0:
        return False

    ladder_active_status = redis.getset(f"status:region:{ladder['regionNo']}:ladder:{ladder['number']}:active", 1)
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


def get_min_active_ladder_no(region_no):
    active_ladder_count = mongo.ladders.count_documents({"regionNo": region_no, "active": 1})
    if active_ladder_count > 0:
        return mongo.ladders.find({"regionNo": region_no, "active": 1}).sort("number", 1).limit(1)[0]["number"]
    else:
        return mongo.ladders.find({"regionNo": region_no, "active": 0}).sort("number", -1).limit(1)[0]["number"]


def get_max_active_ladder_no(region_no):
    active_ladder_count = mongo.ladders.count_documents({"regionNo": region_no, "active": 1})
    if active_ladder_count > 0:
        return (
            mongo.ladders.find({"regionNo": region_no, "active": 1})
            .sort("number", pymongo.DESCENDING)
            .limit(1)[0]["number"]
        )
    else:
        return (
            mongo.ladders.find({"regionNo": region_no, "active": 0})
            .sort("number", pymongo.DESCENDING)
            .limit(1)[0]["number"]
        )


def ladder_task(region_no_list):
    while True:
        try:
            global task_index
            with lock:
                if task_index >= len(region_no_list):
                    task_index = 0
                region_no = region_no_list[task_index]
                task_index += 1

            if redis.setnx(keys.ladder_task_start_time(region_no), datetime.current_time_str()):
                min_active_ladder_no = get_min_active_ladder_no(region_no)
                log.info(f"({region_no}) ladder task start from ladder: {min_active_ladder_no}")
                season = battlenet.get_season_info(region_no)
                log.info(f"({region_no}) current season number: {season['number']}")
                api.post(f"/season/crawler", season)
                redis.set(keys.ladder_task_current_no(region_no), min_active_ladder_no - 10)
            current_ladder_no = redis.incr(keys.ladder_task_current_no(region_no))

            ladder_members = battlenet.get_ladder_members(region_no, current_ladder_no)
            if len(ladder_members) == 0:
                # 成员为空，将 ladder 置为非活跃
                inactive_ladder(region_no, current_ladder_no)

                # 最大 ladder 编号再往后跑 10 个，都不存在则认为任务完成
                max_active_ladder_no = get_max_active_ladder_no(region_no)
                if current_ladder_no > max_active_ladder_no + 10:
                    if redis.lock(keys.ladder_task_done(region_no), timedelta(minutes=5)):
                        task_duration_seconds = datetime.get_duration_seconds(
                            redis.get(keys.ladder_task_start_time(region_no)), datetime.current_time_str()
                        )
                        log.info(
                            f"({region_no}) ladder task done at ladder: {max_active_ladder_no}, duration: {task_duration_seconds}s"
                        )

                        stats.insert(
                            region_no,
                            "ladder_task",
                            {
                                "maxActiveLadderNo": max_active_ladder_no,
                                "duration": task_duration_seconds,
                            },
                        )

                        # 将当前 region 中 team 更新时间早于 ladder job startTime - task duration * 2 且活跃的 team 置为非活跃
                        task_start_time = datetime.get_time(redis.get(keys.ladder_task_start_time(region_no)))
                        for game_mode in [
                            "1v1",
                            "2v2",
                            "3v3",
                            "4v4",
                            "2v2_random",
                            "3v3_random",
                            "4v4_random",
                            "archon",
                        ]:
                            teams_to_inactive = mongo.teams.find(
                                {
                                    "regionNo": region_no,
                                    "gameMode": game_mode,
                                    "updateTime": {
                                        "$lte": datetime.minus(
                                            task_start_time, timedelta(seconds=task_duration_seconds * 2)
                                        )
                                    },
                                    "active": 1,
                                }
                            ).limit(100000)

                            inactive_teams(region_no, game_mode, teams_to_inactive)

                        redis.delete(keys.ladder_task_start_time(region_no))
                        log.info(f"({region_no}) ladder task done success")
                    time.sleep(60)
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
                    try:
                        api.post("/character/batch", ladder_members)
                    except:
                        log.error(f"api character batch error, ladder members count: {len(ladder_members)}")
                        time.sleep(60)

                if not ladder_updated:
                    # 通过新方法未能获取到 ladder 信息
                    inactive_ladder(region_no, current_ladder_no)

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

    region_no_list = []
    for region_no in [1, 2, 3, 5]:
        for _ in range(round(mongo.ladders.count_documents({"regionNo": region_no, "active": 1}) / 100)):
            region_no_list.append(region_no)

    # 遍历天梯成员任务
    for _ in range(10):
        Thread(target=ladder_task, args=(region_no_list,)).start()
    log.info("sczone crawler started")
