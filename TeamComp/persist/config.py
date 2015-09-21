from json import JSONEncoder
import datetime

def __attributes_to_dict(object, fields):
    return {field:getattr(object, field) for field in fields}

def datetime_to_dict(dt):
    return __attributes_to_dict(dt, ('year', 'month', 'day', 'hour', 'minute', 'second'))

def deltatime_to_dict(dt):
    return __attributes_to_dict(dt, ('days', 'seconds'))

class JSONConfigEncoder(JSONEncoder):

    def default(self, o):
        if hasattr(o, '__iter__'):
            return [x for x in o]

        elif isinstance(o, datetime.datetime):
            return datetime_to_dict(o)

        elif isinstance(o, datetime.timedelta):
            return deltatime_to_dict(o)

        return super().default(o)