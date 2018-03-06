import csv
from datetime import datetime, timedelta, timezone
from time import sleep
import logging
import json
import binascii

import baos_knx_parser as knx

from ..config import Config
from .. import datamodel


class AgentWindow(datamodel.Window):

    def __init__(self, start: datetime, agent: str):
        super().__init__(start, agent)

    def process_telegram(self, telegram) -> None:
        self._inc_dict(self.src_addr, str(telegram.src))
        self._inc_dict(self.dest_addr, str(telegram.dest))
        self._inc_dict(self.apci, str(telegram.apci))
        self._inc_dict(self.length, telegram.payload_length)
        self._inc_dict(self.hop_count, telegram.hop_count)
        self._inc_dict(self.priority, str(telegram.priority))

    def _inc_dict(self, d, key):
        d[key] = d.get(key, 0) + 1


class BaseAgent(object):
    """Abstract base class for agent implementations
    """
    LOGGER_NAME = 'AGENT'

    def __init__(self, conf: Config):
        self.conf = conf
        self.log = None
        self.channel = None

        self._init_logger()

    def _init_logger(self):
        self.log = logging.getLogger(self.LOGGER_NAME)

    def get_channel(self):
        if not self.channel:
            self.channel = self.conf.get_amqp_channel()

        return self.channel

    def run(self):
        """Runs the agent
        """
        raise NotImplementedError("run function not implemented")


class SimulatedAgent(BaseAgent):
    """Agent implementation parsing and injecting BAOS-KNX packages into the system.
    It will mock multiple agents, based on filter rules
    """
    LOGGER_NAME = 'SIM-AGENT'

    def __init__(self, conf: Config, log_source: str, agent_filter: {knx.bitmask.Bitmask: str}, log_format: str='old', window_length: timedelta=timedelta(seconds=10), start: datetime=None, end: datetime=None, limit: int=None):
        """Creates a new simulated agent.

        Attributes:
            conf            Config object
            log_source      Path or File-like object to the knx_dump log file
            log_format      Name of the log file's format (either old or new)
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
            window_length   Length of one frame, i.e. for how long the knx
                            packets should be aggregated before sending off
                            to the collector.
                            Defaults to 10 seconds.
            start           Datetime where to begin processing the log
                            Defaults to the very first log entry
            end             Datetime where to stop processing the log
                            Defaults to the very last log entry
            limit           Maximum amount of telegrams to process
                            Defaults to None, indicating no boundary
        """
        super().__init__(conf)

        self.log_source = log_source
        self.log_format = log_format
        self.agent_filter = agent_filter
        self.agent_set = set(agent_filter.values())
        self.window_length = window_length
        self.start = start
        self.end = end
        self.limit = limit

        self.log.info(f"Initialized Simulated Agent for project {self.conf.project_name}")

    def run(self):
        """Runs the simulated agents
        """

        # init connection to AMQP server
        self.get_channel()

        # get generator with telegrams
        if self.log_format == 'old':
            log = self.read_log()
        elif self.log_format == 'new':
            log = self.read_new_log()
        else:
            raise KeyError(f"Unknown log format: {self.log_format}")

        # TODO improve window submission situation. Last window might not be submitted correctly
        next_window = None  # border at which a new frame is started
        windows = None
        for telegram in log:
            if next_window and telegram.timestamp >= next_window:
                self.submit_windows(windows, next_window)
                sleep(0.01)
                windows = None

            if not windows:
                windows = self.setup_new_windows(telegram.timestamp)
                next_window = telegram.timestamp + self.window_length

            for mask, agent in self.agent_filter.items():
                if mask is None or mask == int(telegram.src) or mask == int(telegram.dest):
                    # when mask is None, every traffic matches
                    windows[agent].process_telegram(telegram)

    def submit_windows(self, windows, end: datetime):
        for window in windows.values():
            window.finish(end)
            data = json.dumps(window.to_dict())
            self.channel.basic_publish(exchange=self.conf.name_exchange_agents, routing_key='', body=data)

    def setup_new_windows(self, start: datetime) -> {str: AgentWindow}:
        windows = {}
        for agent in self.agent_set:
            windows[agent] = AgentWindow(start, agent)

        return windows

    def read_log(self):
        """Read the old (eiblog.txt) formatted log."""
        with open(self.log_source) as fp:

            count: int = 0
            for row in csv.reader(fp, delimiter='\t'):
                timestamp = self._parse_csv_date(' '.join(row[0:2]))

                # seek the start position
                if self.start and timestamp < self.start:
                    continue

                if self.limit and count > self.limit:
                    # quit on limit
                    break
                if self.end and timestamp >= self.end:
                    # quit on end
                    break
                else:
                    telegram = knx.parse_knx_telegram(bytes.fromhex(row[5]), timestamp)
                    count += 1
                    yield telegram

    def read_new_log(self):
        """Read the new generated log."""
        with open(self.log_source) as fp:

            count: int = 0
            for row in csv.reader(fp, delimiter=';', quotechar='"'):
                timestamp = self._parse_new_csv_date(row[0])

                # seek the start position
                if self.start and timestamp < self.start:
                    continue

                if self.limit and count > self.limit:
                    # quit on limit
                    break
                if self.end and timestamp >= self.end:
                    # quit on end
                    break
                else:
                    telegram = knx.parse_knx_telegram(binascii.unhexlify(row[1][2:-1]), timestamp)
                    count += 1
                    yield telegram

    def _parse_csv_date(self, date):
        parsed = datetime.strptime(date, '%H:%M:%S %Y-%m-%d')
        parsed.replace(tzinfo=timezone.utc)
        return parsed

    def _parse_new_csv_date(self, date):
        parsed = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        parsed.replace(tzinfo=timezone.utc)
        return parsed
