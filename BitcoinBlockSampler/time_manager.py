## Some function for convenient
import datetime

tz_seoul = datetime.timezone(datetime.timedelta(hours=9))
tz_utc = datetime.timezone(datetime.timedelta())

def get_time(timestamp, tz=tz_utc):
    return datetime.datetime.fromtimestamp(timestamp, tz=tz)


def get_unixtime(timestr):
    return int(datetime.datetime.fromisoformat(timestr).timestamp())