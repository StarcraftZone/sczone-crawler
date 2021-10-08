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
