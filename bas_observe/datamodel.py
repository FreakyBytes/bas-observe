"""
Package containing all common data model classes for BOb
"""
from datetime import datetime

from . import misc


class Window(object):
    """Analystic window
    """

    def __init__(self, start: datetime, agent: str, end: datetime=None):
        self.start: datetime = start
        self.end: datetime = end
        self.agent: str = agent
        self.finished: bool = False if not end else True

        self.src_addr = {}
        self.dest_addr = {}
        self.apci = {}
        self.length = {}
        self.hop_count = {}
        self.priority = {}

    def finish(self, end: datetime) -> None:
        if self.finished:
            raise ValueError("Cannot finish a window that is already finished")

        self.finish = True
        self.end = end

    def to_dict(self) -> {}:
        return {
            'agent': self.agent,
            'start': misc.format_datetime(self.start),
            'end': misc.format_datetime(self.end),
            'src': self.src_addr,
            'dest': self.dest_addr,
            'apci': self.apci,
            'length': self.length,
            'hop_count': self.hop_count,
            'priority': self.priority,
        }

    @classmethod
    def from_dict(cls, d):
        """initiates a window from a dict
        """

        window = cls(start=misc.parse_datetime(d['start']), agent=d['agent'])
        window.end = misc.parse_datetime(d['end']) if d.get('end', None) else None
        window.finished = False if not window.end else True

        window.src_addr = d.get('src', {})
        window.dest_addr = d.get('dest', {})
        window.apci = d.get('apci', {})
        window.length = d.get('length', {})
        window.hop_count = d.get('hop_count', {})
        window.priority = d.get('priority', {})

        return window
