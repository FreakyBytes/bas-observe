"""
Analyser module, which utilizes the Local Outlier Factor to determine
"""
from datetime import datetime
import json

from .base import BaseAnalyser
from .. import datamodel, misc


class LofAnalyser(BaseAnalyser):
    LOGGER_NAME = 'LOF ANALYSER'

    def train(self, start: datetime, end: datetime):
        pass

    def analyse(self):
        # load the model
        self.load_model()
        if not self.model:
            self.model = {}

        # get the AMQP channel and subscribe to relevant topics
        self.log.info("Connect to AMQP server")
        channel = self.get_channel()
        channel.basic_consume(self.on_message, queue=self.conf.name_queue_analyser_lof, no_ack=False)

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
        pass
