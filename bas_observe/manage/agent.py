import csv
import binascii
from datetime import datetime, timedelta
import logging

import baos_knx_parser as knx

from ..config import Config


class Agent(object):
    """Abstract base class for agent implementations
    """

    def __init__(self, conf: Config):
        self.conf = conf
        self.channel = None

        self._init_logger()

    def _init_logger(self):
        self.log = logging.getLogger('Agent')

    def get_channel(self):
        if not self.channel:
            self.channel = self.conf.get_amqp_channel()

        return self.channel

    def run(self):
        """Runs the agent
        """
        raise NotImplementedError("run function not implemented")


class SimulatedAgent(Agent):
    """Agent implementation parsing and injecting BAOS-KNX packages into the system.
    It will mock multiple agents, based on filter rules
    """

    def __init__(self, conf: Config, log_source: str, agent_filter: {}, frame_length: timedelta=timedelta(seconds=10), start: datetime=None, end: datetime=None):
        """Creates a new simulated agent

        Attributes:
            conf            Config object
            log_source      Path or File-like object to the knx_dump log file
            agent_filter    Filter determining which addresses can be read by
                            different simulated agents.
                            In the format:
                            ```
                            {
                                knx.bitmask.Bitmask: 'agent1',
                                knx.bitmask.Bitmask: 'agent1',
                                knx.bitmask.Bitmask: 'agent2',
                            }
                            ```
            frame_length    Length of one frame, i.e. for how long the knx
                            packets should be aggregated before sending off
                            to the collector.
                            Defaults to 10 seconds.
            start           Datetime where to begin processing the log
                            Defaults to the very first log entry
            end             Datetime where to stop processing the log
                            Defaults to the very last log entry
        """
        super().__init__(conf)

        self.log_source = log_source
        self.agent_filter = agent_filter
        self.frame_length = frame_length
        self.start = start
        self.end = end

        self.log.info("Initialized Simulated Agent for project {}", self.conf.project_name)

    def _init_logger(self):
        self.log = logging.getLogger('SimulatedAgent')

    def run(self):
        """Runs the simulated agents
        """
        pass

    def _seek_start(self, fp) -> bool:
        """Tries to seek the start datetime in the log

        Returns True, if it was found, otherwise False
        """

        if not self.start:
            return False

        for row in csv.reader(fp, delimiter='\t'):
            timestamp = datetime.strptime(' '.join(row[0:2]), '%H:%M:%S %Y-%m-%d')
            if timestamp >= self.start:
                return True

        return False
