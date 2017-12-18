"""
Abstract base implementation of an analyser class
"""
import logging
from datetime import datetime

from ..config import Config


class BaseAnalyser(object):
    """Abstract base implementation of an analyser class"""
    LOGGER_NAME = 'ANALYSER'

    def __init__(self, conf: Config):
        self.conf = conf
        self.log = None
        self.channel = None
        self.influxdb = None

        self._init_log()

    def _init_log(self):
        self.log = logging.getLogger(self.LOGGER_NAME)

    def get_channel(self):
        if not self.channel:
            self.channel = self.conf.get_amqp_channel()

        return self.channel

    def train(self, start: datetime, end: datetime):
        raise NotImplemented("train function is not implemented")

    def analyse(self):
        raise NotImplemented("analyse function is not implemented")
