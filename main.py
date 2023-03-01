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

        # api 有定时任务更新，无需从这里更新
        # api.post(
        #     f"/team/batch/inactive",
        #     {"regionNo": region_no, "gameMode": game_mode, "codes": team_codes},
        # )
        log.info(region_no, f"total inactive {len(team_codes)} teams")


def update_ladder(ladder_no, character):
    ladder, teams, status_code = battlenet.get_ladder_and_teams(
        character["regionNo"], character["realmNo"], character["profileNo"], ladder_no
    )

    if status_code is None or ladder is None or len(teams) == 0:
        return False, status_code

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

    return True, status_code


def get_min_active_ladder_no(region_no) -> int:
    active_ladder_count = mongo.ladders.count_documents({"regionNo": region_no, "active": 1})
    if active_ladder_count > 0:
        return mongo.ladders.findOne({"regionNo": region_no, "active": 1}, sort=[("number", pymongo.ASCENDING)])["number"]
    else:
        return mongo.ladders.findOne({"regionNo": region_no, "active": 0}, sort=[("number", pymongo.DESCENDING)])["number"]


def get_max_active_ladder_no(region_no) -> int:
    active_ladder_count = mongo.ladders.count_documents({"regionNo": region_no, "active": 1})
    if active_ladder_count > 0:
        return mongo.ladders.findOne({"regionNo": region_no, "active": 1}, sort=[("number", pymongo.DESCENDING)])["number"]
    else:
        return mongo.ladders.findOne({"regionNo": region_no, "active": 0}, sort=[("number", pymongo.DESCENDING)])["number"]


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
                if season:
                    log.info(region_no, f"current season number: {season['number']}")
                    api.post(f"/season/crawler", season)
                else:
                    log.error(region_no, "get season info error")
                redis.set(keys.ladder_task_current_no(region_no), min_active_ladder_no - 12)
            current_ladder_no = redis.incr(keys.ladder_task_current_no(region_no))

            ladder_members, status_code = battlenet.get_ladder_members(region_no, current_ladder_no)
            if status_code is None:
                log.info(region_no, f"sleep 60s")
                time.sleep(60)
                continue

            if len(ladder_members) > 0:
                # 测试是否是正常数据（通过第一个 member 获取 ladder 数据）
                ladder_updated, status_code = update_ladder(current_ladder_no, ladder_members[0])
                if status_code is None:
                    log.info(region_no, f"sleep 60s")
                    time.sleep(60)
                    continue
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
                            log.error(region_no, f"api character batch error, ladder members count: {len(ladder_members)}")
                            time.sleep(60)

                else:
                    # 通过新方法未能获取到 ladder 信息
                    log.info(region_no, f"legacy ladder: {current_ladder_no}")
                    inactive_ladder(region_no, current_ladder_no)
            elif status_code < 500:
                # 成员为空，将 ladder 置为非活跃
                log.info(region_no, f"empty ladder: {current_ladder_no}")
                inactive_ladder(region_no, current_ladder_no)

                # 最大 ladder 编号再往后跑 12 个，都不存在则认为任务完成
                max_active_ladder_no = get_max_active_ladder_no(region_no)
                if current_ladder_no > max_active_ladder_no + 12:
                    if redis.lock(keys.ladder_task_done(region_no), timedelta(minutes=5)):
                        # 这个 duration 有时候不可靠，过小，导致的 iops 高
                        ladder_task_start_time = redis.get(keys.ladder_task_start_time(region_no))
                        if ladder_task_start_time:
                            task_duration_seconds = datetime.get_duration_seconds(
                                ladder_task_start_time, datetime.current_time_str()
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

                        # 将当前 region 中 team 更新时间超过12小时且活跃的 team 置为非活跃
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
                                    "updateTime": {"$lte": datetime.minus(datetime.current_time(), timedelta(hours=12))},
                                    "active": 1,
                                }
                            ).limit(10000)

                            # api 有专门的定时任务去更新 active 状态；mongo 上的 active 更新意义不大，感觉可以不处理
                            inactive_teams(region_no, game_mode, teams_to_inactive)

                        redis.delete(keys.ladder_task_start_time(region_no))
                        log.info(region_no, f"ladder task done success")
                    log.info(region_no, f"sleep 60s")
                    time.sleep(60)
        except:
            log.error(None, "task loop error")
            log.error(None, traceback.format_exc())
            # 出错后，休眠 1 分钟
            log.info(None, f"sleep 60s")
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
        log.error(None, "mongo create_index error")
        log.error(None, traceback.format_exc())

    region_no_list = [1, 1, 2, 2, 3]

    # 遍历天梯成员任务
    threads = config.getint("app", "threadCount")
    for _ in range(threads):
        Thread(target=ladder_task, args=(region_no_list,)).start()
    log.info(None, f"sczone crawler started, threads: {threads}")
