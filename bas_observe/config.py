"""
BAS OBserve

config module.
"""
import sys
import logging
import urllib.parse

from attr import attrs, attrib
import pika
import influxdb

from .queue import declare_amqp_pipeline


@attrs
class Config(object):
    """Data class holding the main config parameters for BAS Observe

    Attributes:
        project_name        Name of the observation project. Used to determine AMQP topics and InfluxDB database
        amqp_url            URL to the AMQP/RabbitMQ server
        influxdb_url        URL to the InfluxDB server

    """

    project_name = attrib()  # type: str
    amqp_url = attrib()  # type: str
    influxdb_url = attrib()  # type: str

    def __init___(self, *args, **kwargs):
        super(Config, self).__init__(*args, **kwargs)

        self._amqp_connection = None
        self._influxdb_connection = None

    def parse_influxdb_url(self):
        url = urllib.parse.urlparse(self.amqp_url)
        if url['scheme'] not in ('http', 'https', 'udp'):
            raise ValueError("Only http, https, and udp are supported as protocoll for InfluxDB")

        result = {
            'scheme': url['scheme'],
            'host': url['hostname'],
            'user': url['username'],
            'pass': url['password'],
        }
        if url['port']:
            result['port'] = int(url['port'])
        elif self._influxdb_proto == 'https':
            result['port'] = 443
        elif self._influxdb_proto == 'http':
            result['port'] = 8086
        elif self._influxdb_proto == 'udp':
            result['port'] = 4444

        if url['path']:
            result['db'] = url['path']
        else:
            result['db'] = 'bob_{name}'.format(name=self.project_name)

        return result

    def get_amqp_connection(self):
        if not self._amqp_connection:
            self._amqp_connection = pika.BlockingConnection(pika.URLParameters(self.amqp_url))

        return self._amqp_connection

    def get_amqp_channel(self):
        connection = self.get_amqp_connection()
        channel = connection.get_channel()
        # just in case declare the pipelines every time a new channel is opened
        declare_amqp_pipeline(self, channel)
        return channel

    def get_influxdb_connection(self):
        if not self._influxdb_connection:
            param = self.parse_influxdb_url()
            self._influxdb_connection = influxdb.InfluxDBClient(
                host=param['host'],
                port=param['port'],
                ssl=True if param['scheme'] == 'https' else False,
                username=param['user'],
                password=param['pass'],
                database=param['db'],
                use_udp=True if param['scheme'] == 'udp' else False,
                udp_port=param['port']
            )

        return self._influxdb_connection


def setup_logging(level=logging.WARN, logfile=None):
    log_root = logging.getLogger()
    log_root.setLevel(level)
    log_format = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

    # setting up logging to file
    if logfile:
        log_file_handler = logging.FileHandler(logfile)
        log_file_handler.setFormatter(log_format)
        log_root.addHandler(log_file_handler)

    # setting up logging to stdout
    log_stream_handler = logging.StreamHandler(sys.stdout)
    log_stream_handler.setFormatter(log_format)
    log_root.addHandler(log_stream_handler)

    # get the logger for this application
    # log = logging.getLogger('')
    # log.setLevel(logging.INFO)
