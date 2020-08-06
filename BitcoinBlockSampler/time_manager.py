## Some function for convenient
import datetime

tz_seoul = datetime.timezone(datetime.timedelta(hours=9))
tz_utc = datetime.timezone(datetime.timedelta())

def get_time(timestamp):
    return datetime.datetime.fromtimestamp(timestamp, tz=tz_seoul)


def get_unixtime(timestr):
    return int(datetime.datetime.fromisoformat(timestr).timestamp())