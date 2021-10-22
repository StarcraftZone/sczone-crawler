import time
import traceback

import requests

from utils import config, datetime, keys, log, redis


def get_access_token():
    access_token = redis.get("token:battlenet")
    if access_token is None:
        bnetClientId = config.credentials["bnetClientId"]
        bnetClientSecret = config.credentials["bnetClientSecret"]
        response = requests.post(
            "https://www.battlenet.com.cn/oauth/token",
            auth=(bnetClientId, bnetClientSecret),
            data={"grant_type": "client_credentials"},
        )
        response_data = response.json()
        access_token = response_data["access_token"]
        expires_in = response_data["expires_in"]
        redis.setex("token:battlenet", expires_in, access_token)
        log.info("fresh access token: " + access_token)
    return access_token


def get_api_response(path):
    url = f"https://gateway.battlenet.com.cn{path}?locale=en_US&access_token={get_access_token()}"
    response = requests.get(url)
    redis.incr(keys.stats_battlenet_api_request())
    try:
        if response.status_code == 200:
            response_data = response.json()
            return response_data
        elif response.status_code != 404 and response.status_code != 400:
            log.error(f"请求出错 {response.status_code}, {response.text}, url: {url}")
    except:
        log.error(f"请求出错:")
        log.error(traceback.format_exc())
        time.sleep(10)
    return None


def get_league(localized_game_mode):
    parts = localized_game_mode.split(" ")
    if len(parts) > 1:
        return parts[-1].lower()
    return ""


def get_game_mode(localized_game_mode):
    parts = localized_game_mode.split(" ")
    if len(parts) > 1:
        return "_".join(parts[0:-1]).lower()
    return localized_game_mode.lower()


def get_team_code(region_no, game_mode, team_members):
    team_members.sort(key=lambda team_member: int(team_member["profileNo"]))
    result = f"{region_no}_{game_mode}"
    for team_member in team_members:
        result += f"_{team_member['profileNo']}"
    if game_mode == "1v1" and len(team_members) == 1 and "favoriteRace" in team_members[0]:
        result += f"_{team_members[0]['favoriteRace'].lower()}"
    return result


def get_rate(value, total):
    if total == 0:
        return 0.00
    else:
        return round(value * 100 / total, 2)


def get_valid_mmr(team):
    if "mmr" in team:
        if team["mmr"] > 2147483647:
            return -1
        return team["mmr"]
    return 0


# 获取角色下所有天梯
def get_character_all_ladders(region_no, realm_no, profile_no):
    response = get_api_response(f"/sc2/profile/{region_no}/{realm_no}/{profile_no}/ladder/summary")
    ladders = []
    if response is not None:
        for membership in response["allLadderMemberships"]:
            ladders.append(
                {
                    "code": f"{region_no}_{membership['ladderId']}",
                    "number": int(membership["ladderId"]),
                    "regionNo": region_no,
                    "league": get_league(membership["localizedGameMode"]),
                    "gameMode": get_game_mode(membership["localizedGameMode"]),
                }
            )
    return ladders


# 获取天梯信息（过时接口）
def get_ladder_members(region_no, ladder_no):
    response = get_api_response(f"/sc2/legacy/ladder/{region_no}/{ladder_no}")
    members = []
    if response is not None:
        for member_info in response["ladderMembers"]:
            character = member_info["character"]
            members.append(
                {
                    "code": f"{character['region']}_{character['realm']}_{character['id']}",
                    "regionNo": character["region"],
                    "realmNo": character["realm"],
                    "profileNo": int(character["id"]),
                    "displayName": character["displayName"],
                    "clanTag": character["clanTag"] if "clanTag" in character else None,
                    "clanName": character["clanName"] if "clanName" in character else None,
                }
            )
    return members


# 获取指定天梯中所有队伍
def get_ladder_and_teams(region_no, realm_no, profile_no, ladder_no):
    response = get_api_response(f"/sc2/profile/{region_no}/{realm_no}/{profile_no}/ladder/{ladder_no}")
    teams = []
    if response is not None and "currentLadderMembership" in response:
        ladder = {
            "code": f"{region_no}_{ladder_no}",
            "number": ladder_no,
            "regionNo": region_no,
            "league": get_league(response["currentLadderMembership"]["localizedGameMode"]),
            "gameMode": get_game_mode(response["currentLadderMembership"]["localizedGameMode"]),
        }
        for team in response["ladderTeams"]:
            if get_valid_mmr(team) <= 0:
                continue
            team_members = []
            for team_member in team["teamMembers"]:
                team_members.append(
                    {
                        "code": f"{team_member['region']}_{team_member['realm']}_{team_member['id']}",
                        "regionNo": team_member["region"],
                        "realmNo": team_member["realm"],
                        "profileNo": int(team_member["id"]),
                        "displayName": team_member["displayName"],
                        "clanTag": team_member["clanTag"] if "clanTag" in team_member else None,
                        "favoriteRace": team_member["favoriteRace"].lower() if "favoriteRace" in team_member else None,
                    }
                )
            teams.append(
                {
                    "code": get_team_code(region_no, ladder["gameMode"], team_members),
                    "ladderCode": ladder["code"],
                    "regionNo": region_no,
                    "gameMode": ladder["gameMode"],
                    "league": ladder["league"],
                    "points": team["points"],
                    "wins": team["wins"],
                    "losses": team["losses"],
                    "total": team["wins"] + team["losses"],
                    "winRate": get_rate(team["wins"], team["wins"] + team["losses"]),
                    "mmr": get_valid_mmr(team),
                    "joinLadderTime": datetime.get_time_from_timestamp(team["joinTimestamp"]),
                    "teamMembers": team_members,
                }
            )
        return (ladder, teams)
    return (None, [])
