import requests
from .config_helper import config
from .redis_helper import redis_connection


def get_access_token():
    access_token = redis_connection.get("sczone-crawler:token:battlenet")
    if access_token is not None:
        print("cached access token: " + access_token)
        return access_token

    bnetClientId = config["credentials"]["bnetClientId"]
    bnetClientSecret = config["credentials"]["bnetClientSecret"]
    response = requests.post(
        "https://www.battlenet.com.cn/oauth/token",
        auth=(bnetClientId, bnetClientSecret),
        data={"grant_type": "client_credentials"},
    )
    response_data = response.json()
    redis_connection.set("sczone-crawler:token:battlenet", response_data["access_token"], response_data["expires_in"])
    print("fresh access token: " + access_token)
    return response_data["access_token"]
