from utils import datetime


def ladder_task_start_time(region_no):
    return f"task:ladder:region:{region_no}:starttime"


def ladder_task_current_no(region_no):
    return f"task:ladder:region:{region_no}:currentno"


def ladder_task_done(region_no):
    return f"task:ladder:region:{region_no}:done"


def stats_battlenet_api_request():
    return f"stats:battlenet_api_request:{datetime.current_date_str()}"
