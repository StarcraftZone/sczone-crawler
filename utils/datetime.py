from datetime import datetime


def current_time() -> datetime:
    return datetime.now()


def current_time_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_time(time_str: str) -> datetime:
    return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")


def get_time_str(time: datetime) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def get_duration_seconds(time_start: str, time_end: str) -> int:
    delta = get_time(time_end) - get_time(time_start)
    return delta.seconds
