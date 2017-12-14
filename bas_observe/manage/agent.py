import csv
from datetime import datetime, timedelta
import logging

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


class SimulatedAgent(BaseAgent):
    """Agent implementation parsing and injecting BAOS-KNX packages into the system.
    It will mock multiple agents, based on filter rules
    """

    def __init__(self, conf: Config, log_source: str, agent_filter: {knx.bitmask.Bitmask: str}, window_length: timedelta=timedelta(seconds=10), start: datetime=None, end: datetime=None, limit: int=None):
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
        self.agent_filter = agent_filter
        self.agent_set = set(agent_filter.values())
        self.window_length = window_length
        self.start = start
        self.end = end
        self.limit = limit

        self.log.info("Initialized Simulated Agent for project {}", self.conf.project_name)

    def _init_logger(self):
        self.log = logging.getLogger('SimulatedAgent')

    def run(self):
        """Runs the simulated agents
        """

        # init connection to AMQP server
        self.get_channel()

        # get generator with telegrams
        log = self.read_log()

        # TODO improve window submission situation. Last window might not be submitted correctly
        next_window = None  # border at which a new frame is started
        windows = None
        for telegram in log:
            if telegram.timestamp >= next_window:
                self.submit_windows(windows, next_window)
                windows = None

            if not windows:
                windows = self.setup_new_windows(telegram.timestamp)
                next_window = telegram.timestamp + self.window_length

            for mask, agent in self.agent_filter.items():
                if mask == int(telegram.src):
                    windows[agent].process_telegram(telegram)
                if mask == int(telegram.dest):
                    windows[agent].process_telegram(telegram)

    def submit_windows(self, windows, end: datetime):
        for window in windows.values():
            window.finish(end)
            json = window.to_dict()
            self.channel.basic_publish(exchange=self.conf.name_exchange_agents, routing_key='', body=json)

    def setup_new_windows(self, start: datetime) -> {str: AgentWindow}:
        windows = {}
        for agent in self.agent_set:
            windows[agent] = AgentWindow(start, agent)

        return windows

    def read_log(self):

        with open(self.log_source) as fp:
            # jump to the start
            self._seek_start(fp)
            count: int = 0

            for row in csv.reader(fp, delimiter='\t'):
                timestamp = self._parse_csv_date(' '.join(row[0:2]))

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

    def _parse_csv_date(self, date):
        return datetime.strptime(date, '%H:%M:%S %Y-%m-%d')

    def _seek_start(self, fp) -> bool:
        """Tries to seek the start datetime in the log

        Returns True, if it was found, otherwise False
        """

        if not self.start:
            return False

        for row in csv.reader(fp, delimiter='\t'):
            timestamp = self._parse_csv_date(' '.join(row[0:2]))
            if timestamp >= self.start:
                return True

        return False
