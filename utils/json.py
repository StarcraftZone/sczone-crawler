import datetime
import json

import pytz


def json_serial(obj):
    if isinstance(obj, datetime.datetime):
        return obj.astimezone(pytz.timezone("Asia/Shanghai")).isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def dumps(data):
    return json.dumps(data, default=json_serial)


def loads(str):
    return json.loads(str)
