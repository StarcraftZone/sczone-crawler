import requests

from utils import config, json, log

headers = {"token": config.app["apiToken"], "Content-Type": "application/json"}


def request(method, path, data=None):
    url = f"{config.app['apiOrigin']}{path}"
    response = getattr(requests, method)(url, headers=headers, data=json.dumps(data) if data is not None else None, timeout=60)
    if response.status_code == 200:
        response_data = response.json()
        if response_data["code"] == 0:
            return response_data["data"] if "data" in response_data else None
        else:
            log.error(None, f"请求出错: {method} {url}, code: {response_data['code']}, response: {response.text}")
            return None
    else:
        log.error(None, f"请求出错: {method} {url}, status code: {response.status_code}, response: {response.text}")
        return None


def get(path):
    return request("get", path)


def post(path, data):
    return request("post", path, data)


def put(path, data):
    return request("put", path, data)


def patch(path, data):
    return request("patch", path, data)
