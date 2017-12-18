import logging
import json
from datetime import datetime
from collections import OrderedDict

from ..config import Config
from .. import datamodel, misc


class CollectorWindow(datamodel.Window):

    @classmethod
    def from_dict(cls, d: {}):
        return super(CollectorWindow, cls).from_dict(d)

    def influxdb_json(self, project_name: str) -> {}:
        # time_str = misc.format_datetime(self.start)
        time_str = self.start.isoformat()
        data = [
            {
                'time': time_str,
                'measurement': 'window_length',
                'tags': {
                    'project': project_name,
                    'agent': self.agent,
                },
                'fields': {
                    # 'end': misc.format_datetime(self.end),
                    'end': self.end.isoformat(),
                    'length': (self.end - self.start).seconds,
                }
            }
        ]
        for field in ('src_addr', 'dest_addr', 'apci', 'length', 'hop_count', 'priority'):
            value = getattr(self, field)
            if not value:
                # skip fields with empty values
                print(f"skipped {field} because '{value}' seems empty")
                continue

            data.append({
                'time': time_str,
                'measurement': field,
                'tags': {
                    'project': project_name,
                    'agent': self.agent,
                },
                'fields': value
            })

        return data


class Collector(object):
    LOGGER_NAME = 'COLLECTOR'

    def __init__(self, conf: Config, agent_set: set):
        """Inits the collector, which is responsible of aggregating the messages
        from the agents and sending them off to the analysers

        Attributes:
            con                 Config object
            agent_set           Set of all agent names
        """

        self.conf = conf
        self.log = None
        self.channel = None
        self.influxdb = None
        self.agent_set = agent_set

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

    def run(self):
        """Runs the collector"""
        self.log.info("Started collector. Setting up connections...")

        # get the AMQP channel and subscribe to relevant topics
        channel = self.get_channel()
        channel.basic_consume(self.on_agent_message, queue=self.conf.name_queue_agents, no_ack=False)

        # get influxdb client
        self.get_influxdb()

        # run the loop
        try:
            self.log.info("Start waiting for messages")
            self.setup_relay_timeout(channel)
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
        finally:
            self.conf._amqp_connection.close()

    def setup_relay_timeout(self, channel=None):
        if not channel:
            channel = self.channel

        channel.add_timeout(self.conf.relay_timeout, self.relay_messages)

    def on_agent_message(self, channel, method, properties, body):

        window = CollectorWindow.from_dict(json.loads(body))
        self.log.debug(f"Got new message from agent {window.agent} from {window.start} to {window.end}")

        try:
            data = window.influxdb_json(self.conf.project_name)
            self.log.debug(data)
            self.get_influxdb().write_points(data)

            # ack message
            channel.basic_ack(delivery_tag=method.delivery_tag)
        finally:
            pass

    def relay_messages(self):
        try:
            pass
            # TODO check if all windows are in the database
            # TODO relay all windows to the analysers
        finally:
            self.setup_relay_timeout()

    def _get_unrelayed_windows(self):
        """Gets the latest unrelayed window messages ordered around a mean timestamp
        """
        windows = OrderedDict()
        result = self.influxdb.query(
            'SELECT "end", "agent" FROM "window_length" WHERE project = \'{project}\' GROUP BY "agent" ORDER BY time DESC LIMIT {limit}'.format(
                limit=10,
                project=self.conf.project_name,
            )
        )

        for row in result.get_points('window_length'):
            # check if start time is already in the dict
            time = misc.parse_influxdb_datetime(row['time'])
            key = misc.get_uncertain_date_key(windows, time)

            if not key:
                # date is not yet in the dict
                windows[time] = [(time, row['agent'])]
            else:
                # entry already exists, so add this row as well
                windows[time].append((time, row['agent']))
                # recalc key timestamp
                new_key = datetime.fromtimestamp(sum([e[0].timestamp() for e in windows[time]]) / len(windows[time]))
                windows[new_key] = windows[time]
                del windows[time]

        return windows
