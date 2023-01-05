from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

from aleph_core.utils.exceptions import Exceptions

def parse_date_to_timestamp(date: Any) -> int:
    """
    Takes a date in multiple formats and returns the equivalent 
    timestamp in milliseconds
    """
    if isinstance(date, int) or isinstance(date, float):
        if date < 315360000:  # Sunday, December 30, 1979 (seconds)
            return int(now() - date)
        elif date < datetime.now() * 10:
            return int(date * 1000)
        else:
            return int(date)
    elif date is None:
        return now()
    elif isinstance(date, datetime):
        return date.timestamp()
    else:
        raise Exceptions.InvalidDate()


def timestamp_to_string(
        timestamp: int, 
        timezone: str = "UTC", 
        date_format: str = "%Y-%m-%d %H:%M:%S"
        ) -> str:
    """Takes a unix timestamp in milliseconds and returns a string"""
    date = datetime.utcfromtimestamp(timestamp).replace(tzinfo=ZoneInfo("UTC"))
    if timezone != "UTC":
        date = date.astimezone(ZoneInfo(timezone))
    return date.strftime(date_format)


def now() -> int:
    """Current timestamp in milliseconds"""
    return int(datetime.now().timestamp() * 1000)
