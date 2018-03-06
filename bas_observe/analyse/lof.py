"""
Analyser module, which utilizes the Local Outlier Factor to determine
"""
from datetime import datetime
import json
import pandas as pd
import numpy as np

from sklearn.neighbors import LocalOutlierFactor
import baos_knx_parser as knx

from .base import BaseSkLearnAnalyser
from .. import datamodel, misc, vectoriser


APCI_KEYS = list(knx.APCI(None)._attr_map.keys())


class LofAnalyser(BaseSkLearnAnalyser):
    LOGGER_NAME = 'LOF ANALYSER'

    def train(self, start: datetime, end: datetime):
        try:
            self.load_model()
        except:
            self.model = {}

        sys_X = pd.DataFrame()
        agent_X = {}

        window_dict = self.get_windows(start, end)
        for windows in window_dict.values():
            for window in windows:
                vect = [vectoriser.vectorise_window(window)]
                sys_X = sys_X.append(vect)
                agent_X[window.agent] = agent_X.get(window.agent, pd.DataFrame()).append(vect)

        # train all the models!
        self.get_world_model().fit(sys_X)
        for agent, X in agent_X.items():
            self.get_model_for_agent(agent).fit(X)

        self.save_model()

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
            # save the models, since this is a learn-as-you-go thingy
            self.save_model()

    def create_new_model(self):
        return LocalOutlierFactor(n_neighbors=100, algorithm='auto', p=2, contamination=0.1, n_jobs=-1)

    def on_message(self, channel, method, properties, body):

        try:
            windows = [datamodel.Window.from_dict(data_entry) for data_entry in json.loads(body)]
            self.log.info(f"Got new message from collector with {len(windows)} windows")

            data = []
            # fit all windows to the world model
            vects = pd.DataFrame([vectoriser.vectorise_window(window) for window in windows])
            self.log.debug(vects)

            # note: this is the opposite of the Local Outlier Factor
            # cf. https://github.com/scikit-learn/scikit-learn/blob/a24c8b46/sklearn/neighbors/lof.py#L233
            lof_world = self.get_world_model()._decision_function(vects)
            outlier_world = np.ones(vects.shape[0])
            outlier_world[lof_world <= self.get_world_model().threshold_] = -1

            # outlier_world = self.get_world_model()._predict(vects)  # fit_predict(vects)

            for window, vect, outlier, lof in zip(windows, vects, outlier_world, lof_world):
                # -1 means outlier / 1 is an inlier
                # we want to count the amount of outliers, so transform to
                # 1 means outlier / 0 means inlier

                # predict it against the models
                lof_local, = self.get_model_for_agent(window.agent)._decision_function([vect])
                outlier_local = 1 if lof_local <= self.get_model_for_agent(window.agent).threshold_ else -1

                data.append({
                    'time': misc.format_influx_datetime(window.start),
                    'measurement': 'lof',
                    'tags': {
                        'project': self.conf.project_name,
                        'agent': window.agent,
                    },
                    'fields': {
                        'local': 1 if outlier_local < 0 else 0,
                        'local_inlier': 0 if outlier_local < 0 else 1,
                        'local_lof': lof_local * -1,
                        'world': 1 if outlier < 0 else 0,
                        'world_inlier': 0 if outlier < 0 else 1,
                        'world_lof': lof * -1,
                    }
                })

            # write the results to influx
            self.get_influxdb().write_points(data)

            # ack message
            channel.basic_ack(delivery_tag=method.delivery_tag)
        finally:
            pass
