import math
import time
import traceback
from datetime import timedelta
from threading import Lock, Thread

import pymongo
from pymongo import UpdateOne

from utils import api, battlenet, config, datetime, keys, log, redis, stats
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
            log.info(region_no, f"inactive ladder: {ladder_no}")


def inactive_teams(region_no, game_mode, teams):
    team_codes = []
    bulk_operations = []
    for team in teams:
        log.info(region_no, f"inactive team: {team['code']}")
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
        log.info(region_no, f"total inactive {len(team_codes)} teams")


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
            log.info(character["regionNo"], f"found new ladder: {ladder['code']}")

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
        return (
            mongo.ladders.find({"regionNo": region_no, "active": 1})
            .sort("number", pymongo.ASCENDING)
            .limit(1)[0]["number"]
        )
    else:
        return (
            mongo.ladders.find({"regionNo": region_no, "active": 0})
            .sort("number", pymongo.DESCENDING)
            .limit(1)[0]["number"]
        )


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
                log.info(region_no, f"ladder task start from ladder: {min_active_ladder_no}")
                season = battlenet.get_season_info(region_no)
                log.info(region_no, f"current season number: {season['number']}")
                api.post(f"/season/crawler", season)
                redis.set(keys.ladder_task_current_no(region_no), min_active_ladder_no - 12)
            current_ladder_no = redis.incr(keys.ladder_task_current_no(region_no))

            ladder_members = battlenet.get_ladder_members(region_no, current_ladder_no)
            if len(ladder_members) == 0:
                # 成员为空，将 ladder 置为非活跃
                log.info(region_no, f"empty ladder: {current_ladder_no}")
                inactive_ladder(region_no, current_ladder_no)

                # 最大 ladder 编号再往后跑 12 个，都不存在则认为任务完成
                max_active_ladder_no = get_max_active_ladder_no(region_no)
                if current_ladder_no > max_active_ladder_no + 12:
                    if redis.lock(keys.ladder_task_done(region_no), timedelta(minutes=5)):
                        task_duration_seconds = datetime.get_duration_seconds(
                            redis.get(keys.ladder_task_start_time(region_no)), datetime.current_time_str()
                        )
                        log.info(
                            region_no,
                            f"ladder task done at ladder: {max_active_ladder_no}, duration: {task_duration_seconds}s",
                        )

                        stats.insert(
                            region_no,
                            "ladder_task",
                            {
                                "maxActiveLadderNo": max_active_ladder_no,
                                "duration": task_duration_seconds,
                            },
                        )

                        # 将当前 region 中 team 更新时间早于 ladder job startTime - task duration * 3 且活跃的 team 置为非活跃
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
                                            task_start_time, timedelta(seconds=task_duration_seconds * 3)
                                        )
                                    },
                                    "active": 1,
                                }
                            ).limit(100000)

                            inactive_teams(region_no, game_mode, teams_to_inactive)

                        redis.delete(keys.ladder_task_start_time(region_no))
                        log.info(region_no, f"ladder task done success")
                    time.sleep(60)
            else:
                # 测试是否是正常数据（通过第一个 member 获取 ladder 数据）
                ladder_updated = update_ladder(current_ladder_no, ladder_members[0])

                if ladder_updated:
                    # 更新 Character
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

                    if len(bulk_operations) > 0:
                        mongo.characters.bulk_write(bulk_operations)
                        try:
                            api.post("/character/batch", ladder_members)
                        except:
                            log.error(
                                region_no, f"api character batch error, ladder members count: {len(ladder_members)}"
                            )
                            time.sleep(60)

                else:
                    # 通过新方法未能获取到 ladder 信息
                    log.info(region_no, f"legacy ladder: {current_ladder_no}")
                    inactive_ladder(region_no, current_ladder_no)

        except:
            log.error(0, "task loop error")
            log.error(0, traceback.format_exc())
            # 出错后，休眠 1 分钟
            time.sleep(60)


if __name__ == "__main__":
    # 创建 mongo index
    try:
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
    except:
        log.error(0, "mongo create_index error")
        log.error(0, traceback.format_exc())

    # region teams ratio, 4:4:1:3, set to 4:4:1:4 to update faster for CN
    region_no_list = [1, 1, 1, 1, 2, 2, 2, 2, 3, 5, 5, 5, 5]

    # 遍历天梯成员任务
    threads = config.getint("app", "threadCount")
    for _ in range(threads):
        Thread(target=ladder_task, args=(region_no_list,)).start()
    log.info(0, f"sczone crawler started, threads: {threads}")
