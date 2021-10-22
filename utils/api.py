import requests
import traceback
import time
from utils import config, datetime, log, redis, stats, json

headers = {"token": config.app["apiToken"], "Content-Type": "application/json"}


def get(path):
    url = f"{config.app['apiOrigin']}{path}"
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            response_data = response.json()
            return response_data
        else:
            log.error(f"请求出错: {url}, {response.status_code}, {response.text}, url: {url}")
    except:
        log.error(f"请求出错: {url}, ")
        log.error(traceback.format_exc())
        time.sleep(10)
    return None


def post(path, data):
    url = f"{config.app['apiOrigin']}{path}"
    try:
        response = requests.post(url, data=json.dumps(data), headers=headers)
        if response.status_code == 200:
            response_data = response.json()
            return response_data
        else:
            log.error(f"请求出错: {url}, {response.status_code}, {response.text}, url: {url}")
    except:
        log.error(f"请求出错: {url}, ")
        log.error(traceback.format_exc())
        time.sleep(10)
    return None


def put(path, data):
    url = f"{config.app['apiOrigin']}{path}"
    try:
        response = requests.put(url, data=json.dumps(data), headers=headers)
        if response.status_code == 200:
            response_data = response.json()
            return response_data
        else:
            log.error(f"请求出错: {url}, {response.status_code}, {response.text}, url: {url}")
    except:
        log.error(f"请求出错: {url}, ")
        log.error(traceback.format_exc())
        time.sleep(10)
    return None


def patch(path, data):
    url = f"{config.app['apiOrigin']}{path}"
    try:
        response = requests.patch(url, data=json.dumps(data), headers=headers)
        if response.status_code == 200:
            response_data = response.json()
            return response_data
        else:
            log.error(f"请求出错: {url}, {response.status_code}, {response.text}, url: {url}")
    except:
        log.error(f"请求出错: {url}, ")
        log.error(traceback.format_exc())
        time.sleep(10)
    return None
