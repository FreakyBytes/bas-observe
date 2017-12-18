"""
Package containing misc helper functions
"""

from datetime import datetime


_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S%z'


def format_datetime(dt):
    return dt.strftime(_DATETIME_FORMAT)


def parse_datetime(s):
    return datetime.strptime(s, _DATETIME_FORMAT)
