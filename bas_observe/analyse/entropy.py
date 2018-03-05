"""
Analyser module, calculates the entropy for each dimension of the feature vector
"""
from datetime import datetime
import math
import json

import numpy as np
# import pandas as pd
from scipy import stats

from .base import BaseAnalyser
from .. import datamodel, misc, vectoriser


class AddrAnalyser(BaseAnalyser):
    LOGGER_NAME = 'ENTROPY ANALYSER'
    NUM_TIME_BUCKETS = 7 * 24  # one for every hour in the week (actual number of buckets is double this)

    def train(self, start: datetime, end: datetime):
        # bootstrap model data struct
        self.model = {}  # {agent: {buckets: [np.array...], count: np.array}}

        # get the training data
        window_dict = self.get_windows(start, end)
        for windows in window_dict.values():
            for window in windows:
                agent_model = self.model.get(window.agent,
                                             {'buckets': [None] * self.NUM_TIME_BUCKETS * 2,
                                              'count': np.zeros(self.NUM_TIME_BUCKETS * 2)}
                                             )
                vect = vectoriser.vectorise_window(window)
                bucket1, bucket2 = self._get_bucket_by_time(vect[0])

                # vect is truncated, because in [0] the time is encoded
                if not agent_model['buckets'][bucket1]:
                    # if this bucket was not yet filled, put the unmodified vector
                    agent_model['buckets'][bucket1] = vect[1:]
                else:
                    # otherwise casually add the feature vector
                    agent_model['buckets'][bucket1] += vect[1:]

                if not agent_model['buckets'][bucket2]:
                    # if this bucket was not yet filled, put the unmodified vector
                    agent_model['buckets'][bucket2] = vect[1:]
                else:
                    # otherwise casually add the feature vector
                    agent_model['buckets'][bucket2] += vect[1:]

                # increase the counter (to allow calculating the mean later)
                agent_model['count'][bucket1] += 1
                agent_model['count'][bucket2] += 1

        self.save_model()

    def analyse(self):
        # load the model
        self.load_model()
        if not self.model:
            self.model = {}

        # get the AMQP channel and subscribe to relevant topics
        self.log.info("Connect to AMQP server")
        channel = self.get_channel()
        channel.basic_consume(self.on_message, queue=self.conf.name_queue_analyser_entropy, no_ack=False)

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
                agent_model = self.model.get(window.agent,
                                             {'buckets': [None] * self.NUM_TIME_BUCKETS * 2,
                                              'count': np.zeros(self.NUM_TIME_BUCKETS * 2)}
                                             )
                vect = vectoriser.vectorise_window(window)
                bucket1, bucket2 = self._get_bucket_by_time(vect[0])

                entropy1 = stats.entropy(
                    agent_model['buckets'][bucket1] / agent_model['count'][bucket1],
                    vect[1:]
                )
                entropy2 = stats.entropy(
                    agent_model['buckets'][bucket2] / agent_model['count'][bucket2],
                    vect[1:]
                )
                # entropy is a sum (interally) anyway, so sum the both - I guess :D
                entropy = entropy1 + entropy2

                data.append({
                    'time': misc.format_influx_datetime(window.start),
                    'measurement': 'entropy',
                    'tags': {
                        'project': self.conf.project_name,
                        'agent': window.agent,
                    },
                    'fields': {
                        'entropy': entropy,
                        'entropy1': entropy1,
                        'entropy2': entropy2,
                    }
                })

            self.get_influxdb().write_points(data)

            # ack message
            channel.basic_ack(delivery_tag=method.delivery_tag)
        finally:
            pass

    def _get_bucket_by_time(self, time: float):
        """Returns the 2 bucket IDs based on the normalised time"""
        bucket1 = math.floor(time * self.NUM_TIME_BUCKETS) % self.NUM_TIME_BUCKETS
        bucket2 = math.floor((time + (1 / (self.NUM_TIME_BUCKETS * 2))) * self.NUM_TIME_BUCKETS) % self.NUM_TIME_BUCKETS

        return bucket1, bucket2
