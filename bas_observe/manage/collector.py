import logging
import json

from ..config import Config
from .. import datamodel


class CollectorWindow(datamodel.Window):

    @classmethod
    def from_dict(cls, d: {}) -> CollectorWindow:
        return super(CollectorWindow, cls).from_dict(d)

    def influxdb_json(self, project_name: str) -> {}:
        time_str = self.start.isoformat()
        data = [
            {
                'time': time_str,
                'measurement': 'window_length',
                'tags': {
                    'project': project_name,
                    'agent': self.agent,
                },
                'values': {
                    'end': self.end.isoformat(),
                    'length': (self.end - self.start).seconds,
                }
            }
        ]
        for field in ('src_addr', 'dest_addr', 'apci', 'length', 'hop_count', 'priority'):
            data.append({
                'time': time_str,
                'measurement': field,
                'tags': {
                    'project': project_name,
                    'agent': self.agent,
                },
                'values': getattr(self, field)
            })

        return data


class Collector(object):

    def __init__(self, conf: Config, agent_set: set(str)):
        """Inits the collector, which is responsible of aggregating the messages
        from the agents and sending them off to the analysers

        Attributes:
            con                 Config object
            agent_set           Set of all agent names
        """

        self.conf = conf
        self.log = None
        self.channel = None
        self.agent_set = agent_set

        self._init_log()

    def _init_log(self):
        self.log = logging.getLogger('Collector')

    def get_channel(self):
        if not self.channel:
            self.channel = self.conf.get_amqp_channel()

        return self.channel

    def get_influxdb(self):
        if not self.influxdb:
            self.influxdb = self.conf.get_influxdb_connection

        return self.influxdb

    def run(self):
        """Runs the collector"""

        # get the AMQP channel and subscribe to relevant topics
        channel = self.get_channel
        channel.base_consume(self.on_agent_message, queue=self.con.name_queue_agents, no_ack=False)

        # get influxdb client
        self.get_influxdb()

    def on_agent_message(self, channel, method, properties, body):

        window = CollectorWindow.from_dict(json.loads(body))

        try:
            data = window.influxdb_json()
            self.get_influxdb().write_points(data)

            # ack message
            channel.base_ack(delivery_tag=method.delivery_tag)
        finally:
            pass

        # TODO check if all windows are in the database
        # TODO relay all windows to the analysers
