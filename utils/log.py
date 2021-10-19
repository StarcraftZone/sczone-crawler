from utils import datetime


def info(msg):
    print(f"{datetime.current_time_str()} [INFO] {msg}\n", end="")


def error(msg):
    print(f"{datetime.current_time_str()} [ERROR] {msg}\n", end="")
