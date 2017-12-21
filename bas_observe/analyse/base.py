"""
Abstract base implementation of an analyser class
"""
import logging
from datetime import datetime
from collections import OrderedDict
import json

from ..config import Config
from .. import misc, datamodel


class BaseAnalyser(object):
    """Abstract base implementation of an analyser class"""
    LOGGER_NAME = 'ANALYSER'

    def __init__(self, conf: Config, model: str):
        self.conf = conf
        self.model_path = model

        self.log = None
        self.channel = None
        self.influxdb = None
        self.model = None

        self._init_log()

    def _init_log(self):
        self.log = logging.getLogger(self.LOGGER_NAME)

    def get_channel(self):
        if not self.channel:
            self.channel = self.conf.get_amqp_channel()

        return self.channel

    def get_influxdb(self):
        if not self.influxdb:
            self.influxdb = self.conf.get_influxdb_connection()

        return self.influxdb

    def train(self, start: datetime, end: datetime):
        raise NotImplemented("train function is not implemented")

    def analyse(self):
        raise NotImplemented("analyse function is not implemented")

    def load_model(self):
        with open(self.model_path, mode='r') as fp:
            self.model = json.load(fp)

        return self.model

    def save_model(self):
        with open(self.model_path, mode='w') as fp:
            json.dump(self.model, fp, encoding='utf-8')

    def get_windows(self, start: datetime, end: datetime):
        windows = OrderedDict()  # {time: [window, window, ...], time: [...]}

        result = self.get_influxdb().query('SELECT * FROM "agent_status" WHERE "project" = \'{project}\' and time > \'{start}\' and time < \'{end}\' ORDER BY time DESC'.format(
            project=self.conf.project_name,
            start=start.isoformat(),
            end=end.isoformat(),
        ))

        for data in result.get_points('agent_status'):
            # construct window datamodel
            self.log.debug(data)
            window = datamodel.Window(
                misc.parse_influxdb_datetime(data['time']),
                data['agent'],
                misc.parse_influxdb_datetime(data['end'])
            )

            # fill it with the measurements
            window = self._query_measurements(window)

            key = misc.get_uncertain_date_key(windows, window.start)
            if not key:
                windows[window.start] = [window]
            else:
                # entry already exists, so add this row as well
                windows[key].append(window)
                # recalc key timestamp
                new_key = datetime.fromtimestamp(sum([e.start.timestamp() for e in windows[key]]) / len(windows[key]))
                windows[new_key] = windows[key]
                del windows[key]

        return windows

    def _query_measurements(self, window: datamodel.Window):
        queries = []

        for measure in misc.MEASUREMENTS:
            queries.append(
                'SELECT * FROM "{measurement}" WHERE "project" = \'{project}\' and "agent" = \'{agent}\' and time = \'{time}\' LIMIT 1'.format(
                    project=self.conf.project_name,
                    agent=window.agent,
                    time=window.start.isoformat(),
                    measurement=measure,
                )
            )

        self.log.debug(f"Execute InfluxDB queries: \"{'; '.join(queries)}\"")
        result = self.get_influxdb().query('; '.join(queries))
        for resultset in result:
            (measure, group), data = resultset.items()[0]
            data = next(data)
            # writes values to window
            setattr(window, measure, {k: v for k, v in data.items() if k not in ('time', 'project', 'agent')})

        return window
