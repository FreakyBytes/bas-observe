"""
Analyser module, which compares the addresses of in coming packets with a table
of known addresses
"""
from datetime import datetime
import json

from .base import BaseAnalyser
from .. import datamodel, misc


class AddrAnalyser(BaseAnalyser):
    LOGGER_NAME = 'ADDR ANALYSER'

    def train(self, start: datetime, end: datetime):
        # bootstrap model data struct
        self.model = {}  # {agent: {src: set(addrs...), dest: set(addrs...)}}

        # get the training data
        window_dict = self.get_windows(start, end)
        self.log.debug(window_dict)
        for windows in window_dict.values():
            for window in windows:
                if window.agent not in self.model:
                    # bootstrap the model for this agent
                    self.log.debug(f"Bootstrap model entry for agent \"{window.agent}\"")
                    self.model[window.agent] = {'src': set(), 'dest': set()}

                src = [addr for addr, count in window.src_addr.items() if count and count > 0]
                dest = [addr for addr, count in window.dest_addr.items() if count and count > 0]

                self.log.debug(f"Agent {window.agent}, found source addrs: \"{','.join(src)}\"")
                self.log.debug(f"Agent {window.agent}, found destination addrs: \"{','.join(dest)}\"")

                self.model[window.agent]['src'] = self.model[window.agent]['src'].union(src)
                self.model[window.agent]['dest'] = self.model[window.agent]['dest'].union(dest)

        self.save_model()

    def analyse(self):
        # load the model
        self.load_model()
        if not self.model:
            self.model = {}

        # get the AMQP channel and subscribe to relevant topics
        self.log.info("Connect to AMQP server")
        channel = self.get_channel()
        channel.basic_consume(self.on_message, queue=self.conf.name_queue_analyser_addr, no_ack=False)

        # get influxdb client
        self.get_influxdb()

        # run the loop
        try:
            self.log.info("Start waiting for messages")
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
        finally:
            self.conf._amqp_connection.close()

    def on_message(self, channel, method, properties, body):

        try:
            windows = [datamodel.Window.from_dict(data_entry) for data_entry in json.loads(body)]
            self.log.info(f"Got new message from collector with {len(windows)} windows")

            data = []
            for window in windows:
                # get model entry for this agent
                agent_model = self.model.get(window.agent, {'src': set(), 'dest': set()})

                unknown_src_addr = 0
                unknown_src_telegrams = 0
                unknown_dest_addr = 0
                unknown_dest_telegrams = 0

                for addr, amount in window.src_addr.items():
                    if not amount:
                        continue

                    if addr not in agent_model['src']:
                        unknown_src_addr += 1
                        unknown_src_telegrams += amount
                        self.log.warn(f"Found {amount} packets from unknown source address {addr} on agent {window.agent}")

                for addr, amount in window.dest_addr.items():
                    if not amount:
                        continue

                    if addr not in agent_model['dest']:
                        unknown_dest_addr += 1
                        unknown_dest_telegrams += amount
                        self.log.warn(f"Found {amount} packets to unknown destination address {addr} on agent {window.agent}")

                data.append({
                    'time': window.start,
                    'measurement': 'unknown_addr',
                    'tags': {
                        'project': self.conf.project_name,
                        'agent': window.agent,
                    },
                    'fields': {
                        'unknown_src_addr': unknown_src_addr,
                        'unknown_src_telegrams': unknown_src_telegrams,
                        'unknown_dest_addr': unknown_dest_addr,
                        'unknown_dest_telegrams': unknown_dest_telegrams,
                        'unknown_addr': unknown_src_addr + unknown_dest_addr,
                        'unknown_telegrams': unknown_src_telegrams + unknown_dest_telegrams,
                    }
                })

            self.get_influxdb().write_points(data)

            # ack message
            channel.basic_ack(delivery_tag=method.delivery_tag)
        finally:
            pass
