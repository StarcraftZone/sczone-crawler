import requests

from utils import config, datetime, log, redis


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
    redis.incr(f"stats:battlenet-api-request-count:{datetime.current_date_str()}")
    if response.status_code == 200:
        response_data = response.json()
        redis.incr(f"stats:battlenet-api-request-count:{datetime.current_date_str()}")
        return response_data
    elif response.status_code != 404:
        log.info(f"请求出错: {url}, response: {response.status_code}, {response.text}")
    else:
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
    team_members.sort(key=lambda team_member: team_member["id"])
    result = f"{region_no}_{game_mode}"
    for team_member in team_members:
        result += f"_{team_member['id']}"
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


# 获取指定天梯中所有队伍
def get_ladder_all_teams(region_no, realm_no, profile_no, ladder):
    response = get_api_response(f"/sc2/profile/{region_no}/{realm_no}/{profile_no}/ladder/{ladder['number']}")
    teams = []
    if response is not None:
        for team in response["ladderTeams"]:
            team_members = []
            for team_member in team["teamMembers"]:
                team_members.append(
                    {
                        "code": f"{team_member['region']}_{team_member['realm']}_{team_member['id']}",
                        "regionNo": team_member["region"],
                        "realmNo": team_member["realm"],
                        "profileNo": team_member["id"],
                        "displayName": team_member["displayName"],
                        "clanTag": team_member["clanTag"] if "clanTag" in team_member else None,
                        "favoriteRace": team_member["favoriteRace"].lower() if "favoriteRace" in team_member else None,
                    }
                )
            teams.append(
                {
                    "code": get_team_code(region_no, ladder["gameMode"], team["teamMembers"]),
                    "ladderCode": ladder["code"],
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
    return teams
