"""
Package containing misc helper functions
"""

from datetime import datetime, timedelta


MEASUREMENTS = ('src_addr', 'dest_addr', 'apci', 'length', 'hop_count', 'priority')

_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S%z'
_DATETIME_FORMAT_NO_TZ = '%Y-%m-%dT%H:%M:%S'
_DATETIME_ISO_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
_DATETIME_LEGACY_FORMAT = '%Y-%m-%d %H:%M:%S'


def format_datetime(dt: datetime):
    return dt.strftime(_DATETIME_FORMAT)


def format_influx_datetime(dt: datetime):
    return dt.strftime(_DATETIME_LEGACY_FORMAT)


def parse_datetime(s: str):
    try:
        return datetime.strptime(s, _DATETIME_FORMAT)
    except ValueError:
        # let's try without timezone
        return datetime.strptime(s, _DATETIME_FORMAT_NO_TZ)


def parse_influxdb_datetime(s: str):
    for format in (_DATETIME_ISO_FORMAT, _DATETIME_FORMAT_NO_TZ, _DATETIME_LEGACY_FORMAT):
        try:
            return datetime.strptime(s, format)
        except ValueError:
            pass

    # no format seemed to match
    raise ValueError("Could not parse '{date}'. Format does not match any expected one.")


def get_uncertain_date_key(d: {}, timestamp: datetime, delta: timedelta=timedelta(seconds=2)):
    """Returns the first dict key, which lies within delta around the timestamp
    Otherwise returns None
    """
    # TODO actually return the closest one!
    for key in d.keys():
        if abs(key - timestamp) < delta:
            return key

    return None
