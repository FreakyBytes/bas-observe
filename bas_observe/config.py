"""
BAS OBserve

config module.
"""
import sys
import logging
import urllib.parse

from attr import attrs, attrib


@attrs
class Config(object):
    """Data class holding the main config parameters for BAS Observe

    Attributes:
        project_name        Name of the observation project. Used to determine AMQP topics and InfluxDB database
        amqp_url            URL to the AMQP/RabbitMQ server
        influxdb_url        URL to the InfluxDB server

    Properties:
        amqp_host
        amqp_port
        influxdb_host
        influxdb_port
        influxdb_db
    """

    project_name = attrib()  # type: string
    amqp_url = attrib()  # type: string
    influxdb_url = attrib()  # type: string

    def __init___(self, *args, **kwargs):
        super(Config, self).__init__(*args, **kwargs)

        self._amqp_host = None
        self._amqp_port = None

        self._influxdb_proto = None
        self._influxdb_host = None
        self._influxdb_port = None
        self._influxdb_db = None

    def _parse_amqp_url(self):
        url = urllib.parse.urlparse(self.amqp_url)
        if url['scheme'] != 'amqp':
            raise ValueError("Only amqp is supported as protocoll for the AMQP server at the moment")

        self._amqp_host = url['hostname']
        self._amqp_port = int(url['port']) or 5672

    def _parse_influxdb_url(self):
        url = urllib.parse.urlparse(self.amqp_url)
        if url['scheme'] not in ('http', 'https', 'udp'):
            raise ValueError("Only http, https, and udp are supported as protocoll for InfluxDB")

        self._influxdb_proto = url['scheme']
        self._influxdb_host = url['hostname']
        if url['port']:
            self._influxdb_port = int(url['port'])
        elif self._influxdb_proto == 'https':
            self._influxdb_port = 443
        elif self._influxdb_proto == 'http':
            self._influxdb_port = 80
        elif self._influxdb_proto == 'udp':
            self._influxdb_port = 4444

        if url['path']:
            self._influxdb_db = url['path']
        else:
            self._influxdb_db = 'bob_{name}'.format(name=self.project_name)

    @property
    def amqp_host(self):
        if not self._amqp_host:
            self._parse_amqp_url()

        return self._amqp_host

    @property
    def amqp_port(self):
        if not self._amqp_port:
            self._parse_amqp_url()

        return self._amqp_port

    @property
    def influxdb_proto(self):
        if not self._influxdb_proto:
            self._parse_influxdb_url()

        return self._influxdb_proto

    @property
    def influxdb_host(self):
        if not self._influxdb_host:
            self._parse_influxdb_url()

        return self._influxdb_host

    @property
    def influxdb_port(self):
        if not self._influxdb_port:
            self._parse_influxdb_url()

        return self._influxdb_port

    @property
    def influxdb_db(self):
        if not self._influxdb_db:
            self._parse_influxdb_url()

        if not self._influxdb_db:
            return 'bob_{name}'.format(name=self.project_name)
        else:
            return self._influxdb_db


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
