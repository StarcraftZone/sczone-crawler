from configparser import ConfigParser
import redis

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


def delete(key):
    return redis_connection.delete(get_full_key(key))


def exists(key):
    return redis_connection.exists(get_full_key(key))


def incr(key):
    return redis_connection.incr(get_full_key(key))
