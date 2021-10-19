from utils import datetime


def character_task_start_time(region_no):
    return f"task:character:region:{region_no}:starttime"


def character_task_current_no(region_no):
    return f"task:character:region:{region_no}:currentno"


def character_task_done_stats(region_no):
    return f"stats:duration:task:character:region:{region_no}:{datetime.current_time_str_short()}"


def ladder_task_start_time(region_no):
    return f"task:ladder:region:{region_no}:starttime"


def ladder_task_current_no(region_no):
    return f"task:ladder:region:{region_no}:currentno"


def ladder_task_done_stats(region_no):
    return f"stats:duration:task:ladder:region:{region_no}:{datetime.current_time_str_short()}"


def ladder_member_task_done_stats(region_no):
    return f"stats:duration:task:ladder_member:region:{region_no}:{datetime.current_time_str_short()}"


def ladder_member_task_start_time(region_no):
    return f"task:ladder_member:region:{region_no}:starttime"


def ladder_member_task_current_no(region_no):
    return f"task:ladder_member:region:{region_no}:currentno"


def ladder_member_task_done_stats(region_no):
    return f"stats:duration:task:ladder_member:region:{region_no}:{datetime.current_time_str_short()}"
