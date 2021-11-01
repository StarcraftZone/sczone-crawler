import time
import traceback

import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from utils import config, json, log

headers = {"token": config.app["apiToken"], "Content-Type": "application/json"}


def retry_failed(retry_state):
    log.error(f"请求重试失败: {retry_state.args[0]}, {retry_state.args[1]}, {retry_state.outcome.result()}")
    return None


@retry(wait=wait_fixed(3), stop=stop_after_attempt(3), retry_error_callback=retry_failed)
def request(method, path, data=None):
    url = f"{config.app['apiOrigin']}{path}"
    response = getattr(requests, method)(url, headers=headers, data=json.dumps(data) if data is not None else None)
    if response.status_code == 200:
        response_data = response.json()
        if response_data["code"] == 0:
            return response_data["data"] if "data" in response_data else None
        else:
            log.error(f"请求出错: {method} {url}, code: {response_data['code']}, response: {response.text}")
            return None
    else:
        log.error(f"请求出错: {method} {url}, status code: {response.status_code}, response: {response.text}")
        return None


def get(path):
    return request("get", path)


def post(path, data):
    return request("post", path, data)


def put(path, data):
    return request("put", path, data)


def patch(path, data):
    return request("patch", path, data)
