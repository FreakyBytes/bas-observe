"""
Module containing all common data model classes for BOb
"""
from datetime import datetime


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
            'start': str(self.start),
            'end': str(self.end),
            'src': self.src_addr,
            'dest': self.dest_addr,
            'apci': self.apci,
            'length': self.length,
            'hop_count': self.hop_count,
            'priority': self.priority,
        }

    @staticmethod
    def from_dict(d) -> Window:
        # TODO
        return None

    def influxdb_json(self) -> {}:
        # TODO
        return {}
