"""
BAS Observe

Main entrypoint for the command line interface.
Called in __module__.py
"""
import logging

import click

from . import config
from .manage.agent import SimulatedAgent
from .manage.collector import Collector


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
    ctx.obj['LOG'] = logging.getLogger('CLI')  # re initiate logger

    ctx.obj['CONF'] = config.Config(project_name=project, amqp_url=amqp, influxdb_url=influxdb)


@cli.command('simulate', short_help="simulates agents by injecting packets from a log file")
@click.option('-a', '--agent', nargs=3, type=(str, int, int), multiple=True,
              help="defines an agent filter with <AGENT_NAME ADDR_FILTER ADDR_FILTER_MASK>")
@click.pass_context
def simulate(ctx):
    pass


@cli.command('collector', short_help="collects agent windows to InfluxDB and forwards them to the analysers")
@click.option('-a', '--agent', nargs=1, type=str, multiple=True,
              help="defines the list of agents by name")
@click.pass_context
def log(ctx, agent):
    log = ctx.obj['LOG']
    agent = set(agent)
    log.info(f"{len(agent)} agents defined: {', '.join(agent)}")
    log.info("Starting Collector")
    collector = Collector(ctx.obj['CONF'], agent)
    collector.run()


# -----------------------------------------------------------------------------


@cli.group(short_help="starts one of the observation modules")
@click.pass_context
def analyse(ctx):
    pass


@analyse.command('addr', short_help="start address lookup observation")
@click.pass_context
def analyse_addr(ctx):
    pass


@analyse.command('entropy', short_help="start entropy estimation")
@click.pass_context
def analyse_entropy(ctx):
    pass


@analyse.command('lof', short_help="start local outlier factor observation")
@click.pass_context
def analyse_lof(ctx):
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


def run_cli():
    cli(auto_envvar_prefix='BOB', obj={})
