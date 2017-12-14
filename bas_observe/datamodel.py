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

    @classmethod
    def from_dict(cls, d) -> Window:
        """initiates a window from a dict
        """

        window = cls(start=datetime.strptime(d['start'], 'YYYY-MM-DDTHH:MM:SS.mmmmmm'), agent=d['agent'])
        window.end = datetime.strptime(d['end'], 'YYYY-MM-DDTHH:MM:SS.mmmmmm') if d.get('end', None) else None
        window.finished = False if not window.end else True

        window.src_addr = d.get('src', {})
        window.dest_addr = d.get('dest', {})
        window.apci = d.get('apci', {})
        window.length = d.get('length', {})
        window.hop_count = d.get('hop_count', {})
        window.priority = d.get('priority', {})

        return window
