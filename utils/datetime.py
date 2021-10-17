from datetime import datetime
from datetime import timedelta
from time import time


def current_time_mills() -> int:
    return int(round(time() * 1000))


def current_time() -> datetime:
    return datetime.now()


def current_time_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def current_time_str_short() -> str:
    return datetime.now().strftime("%Y%m%d%H%M%S")


def current_date_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def get_time(time_str: str) -> datetime:
    return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")


def get_time_str(time: datetime) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def get_time_from_timestamp(timestamp) -> datetime:
    return datetime.fromtimestamp(timestamp)


def get_duration_seconds(time_start: str, time_end: str) -> int:
    delta = get_time(time_end) - get_time(time_start)
    return delta.seconds


def get_timedelta_mills(timedelta: timedelta) -> int:
    return round(timedelta.total_seconds() * 1000)
