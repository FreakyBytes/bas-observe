"""
Analyser module, which utilizes the Local Outlier Factor to determine
"""
from datetime import datetime
import json

from sklearn.neighbors import LocalOutlierFactor
import baos_knx_parser as knx

from .base import BaseSkLearnAnalyser
from .. import datamodel, misc


APCI_KEYS = list(knx.APCI(None)._attr_map.keys())


class LofAnalyser(BaseSkLearnAnalyser):
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

    def create_new_model(self):
        return LocalOutlierFactor(n_neighbors=20, algorithm='auto', p=2, contamination=0.1)

    def on_message(self, channel, method, properties, body):

        try:
            windows = [datamodel.Window.from_dict(data_entry) for data_entry in json.loads(body)]
            self.log.info(f"Got new message from collector with {len(windows)} windows")

            data = []
            for window in windows:
                # get lof model for this agent
                model = self.get_model_for_agent(window.agent)



            # ack message
            channel.basic_ack(delivery_tag=method.delivery_tag)
        finally:
            pass
