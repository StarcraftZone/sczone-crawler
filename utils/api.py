import time
import traceback

import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from utils import config, json, log

headers = {"token": config.app["apiToken"], "Content-Type": "application/json"}


@retry(wait=wait_fixed(3), stop=stop_after_attempt(3))
def request(method, path, data=None):
    url = f"{config.app['apiOrigin']}{path}"
    response = getattr(requests, method)(url, headers=headers, data=json.dumps(data) if data is not None else None)
    if response.status_code == 200:
        response_data = response.json()
        if response_data["code"] == 0:
            return response_data["data"]
        else:
            log.error(f"请求出错: {url}, code: {response_data['code']}, response: {response.text}")
            return None
    else:
        log.error(f"请求出错: {url}, status code: {response.status_code}, response: {response.text}")
        return None


def get(path):
    try:
        return request("get", path)
    except:
        log.error(f"请求出错: get {path}")
        log.error(traceback.format_exc())
        time.sleep(10)
    return None


@retry(wait=wait_fixed(3), stop=stop_after_attempt(3))
def post(path, data):
    try:
        return request("post", path, data)
    except:
        log.error(f"请求出错: post {path}")
        log.error(traceback.format_exc())
        time.sleep(10)
    return None


def put(path, data):
    try:
        return request("put", path, data)
    except:
        log.error(f"请求出错: put {path}")
        log.error(traceback.format_exc())
        time.sleep(10)
    return None


def patch(path, data):
    try:
        return request("patch", path, data)
    except:
        log.error(f"请求出错: patch {path}")
        log.error(traceback.format_exc())
        time.sleep(10)
    return None
