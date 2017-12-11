"""
BAS Observe

Main entrypoint for the command line interface.
Called in __module__.py
"""
import logging

import click

from . import config


log = logging.getLogger('CLI')


@click.group()
@click.option('--log-file', default=None, help="Writes log output to file")
@click.option('-l', '--log-level', default='INFO', type=click.Choice(['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL']))
@click.option('--project', help="project name")
@click.option('--amqp', default='amqp://localhost:5672', help="URL to the AMQP/RabbitMQ server")
@click.option('--influxdb', default='http://localhost:80', help="URL to the InfluxDB server")
@click.pass_context
def cli(ctx, log_file, log_level, project, amqp, influxdb):
    """
    Bas OBserve (bob)
    """
    config.setup_logging(level=log_level, logfile=log_file)
    log = logging.getLogger('CLI')  # re initiate logger

    conf = config.Config(project_name=project, amqp_url=amqp, influxdb_url=influxdb)
    ctx.conf = conf


@cli.command('simulate', short_help="simulates agents by injecting packets from a log file")
@click.pass_context
def simulate(ctx):
    pass


@cli.command('log', short_help="logs packages and observation results to InfluxDB")
@click.pass_context
def log(ctx):
    pass


# -----------------------------------------------------------------------------


@cli.group(short_help="starts one of the observation modules")
@click.pass_context
def observe(ctx):
    pass


@observe.command('addr', short_help="start address lookup observation")
@click.pass_context
def observe_addr(ctx):
    pass


@observe.command('entropy', short_help="start entropy estimation")
@click.pass_context
def observe_entropy(ctx):
    pass


@observe.command('lof', short_help="start local outlier factor observation")
@click.pass_context
def observe_lof(ctx):
    pass


# -----------------------------------------------------------------------------

@cli.group(short_help="trains one of the observation modules from InfluxDB")
@click.pass_context
def train(ctx):
    pass


@train.command('addr', short_help="gathers an address lookup table of a all address that have communicated")
@click.pass_context
def tain_addr(ctx):
    pass


@train.command('entropy', short_help="determines base line from entropy calculation")
@click.pass_context
def train_entropy():
    pass


@train.command('lof')
@click.pass_context
def train_lof():
    pass
