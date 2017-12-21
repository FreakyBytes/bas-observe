"""
Package containing misc helper functions
"""

from datetime import datetime, timedelta


MEASUREMENTS = ('src_addr', 'dest_addr', 'apci', 'length', 'hop_count', 'priority')

_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S%z'
_DATETIME_ISO_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
_DATETIME_LEGACY_FORMAT = '%Y-%m-%d %H:%M:%S'


def format_datetime(dt: datetime):
    return dt.strftime(_DATETIME_FORMAT)


def parse_datetime(s: str):
    return datetime.strptime(s, _DATETIME_FORMAT)


def parse_influxdb_datetime(s: str):
    try:
        return datetime.strptime(s, _DATETIME_ISO_FORMAT)
    except ValueError:
        # let's try a more legacy date format
        return datetime.strptime(s, _DATETIME_LEGACY_FORMAT)


def get_uncertain_date_key(d: {}, timestamp: datetime, delta: timedelta=timedelta(seconds=2)):
    """Returns the first dict key, which lies within delta around the timestamp
    Otherwise returns None
    """
    for key in d.keys():
        if abs(key - timestamp) < delta:
            return key

    return None
