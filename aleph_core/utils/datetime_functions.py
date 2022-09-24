from aleph_core.utils.exceptions import Exceptions

from datetime import datetime
from zoneinfo import ZoneInfo


def parse_date_to_timestamp(date: any) -> int:
    """Takes a date in multiple formats and returns the equivalent timestamp"""
    if isinstance(date, int) or isinstance(date, float):
        if date < 315360000:  # Sunday, December 30, 1979
            return int(now() - date)
        else:
            return int(date)
    elif isinstance(date, str):
        pass
    elif date is None:
        return now()
    else:
        raise Exceptions.InvalidDate()


def timestamp_to_string(timestamp: int, timezone: str = "UTC", date_format: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Takes a unix timestamp in seconds and returns a string"""
    date = datetime.utcfromtimestamp(timestamp).replace(tzinfo=ZoneInfo("UTC"))
    if timezone != "UTC":
        date = date.astimezone(ZoneInfo(timezone))
    return date.strftime(date_format)


def now() -> int:
    """Current timestamp"""
    return int(datetime.now().timestamp())
