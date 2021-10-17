import redis
from configparser import ConfigParser
from datetime import timedelta
from utils import datetime

config = ConfigParser()
config.read("config.ini", "UTF-8")
redis_connection = redis.Redis(
    host=config["redis"]["host"],
    port=config["redis"]["port"],
    password=config["redis"]["password"],
    decode_responses=True,
)
key_prefix = "sczone-crawler"


def get_full_key(key):
    return f"{key_prefix}:{key}"


def get(key):
    return redis_connection.get(get_full_key(key))


def set(key, value):
    return redis_connection.set(get_full_key(key), value)


def setnx(key, value):
    return redis_connection.setnx(get_full_key(key), value)


def setex(key, time, value):
    return redis_connection.setex(get_full_key(key), time, value)


def expire(key, time):
    return redis_connection.expire(get_full_key(key), time)


def getset(key, value):
    return redis_connection.getset(get_full_key(key), value)


def delete(key):
    return redis_connection.delete(get_full_key(key))


def exists(key):
    return redis_connection.exists(get_full_key(key))


def incr(key):
    return redis_connection.incr(get_full_key(key))


def lock(key, duration: timedelta):
    key = f"lock:{key}"
    value = datetime.current_time_mills() + datetime.get_timedelta_mills(duration)
    status = setnx(key, value)
    if status:
        return True
    old_expire_time = get(key)
    if old_expire_time is None or int(old_expire_time) < datetime.current_time_mills():
        new_expire_time = datetime.current_time_mills() + datetime.get_timedelta_mills(duration)
        current_expire_time = getset(key, new_expire_time)
        if current_expire_time == old_expire_time:
            return True
    return False


def unlock(key):
    return delete(key)
