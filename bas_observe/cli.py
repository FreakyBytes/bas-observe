"""
BAS Observe

Main entrypoint for the command line interface.
Called in __module__.py
"""
import logging
from datetime import datetime, timedelta

import click
import baos_knx_parser as knx

from . import config, misc
from .manage.agent import SimulatedAgent
from .manage.collector import Collector
from .analyse.addr import AddrAnalyser
from .analyse.lof import LofAnalyser
from .analyse.entropy import EntropyAnalyser
from .analyse.svm import SvmAnalyser


@click.group()
@click.option('--log-file', default=None, help="Writes log output to file")
@click.option('-l', '--log-level', default='INFO', type=click.Choice(['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL']))
@click.option('--project', prompt=True, help="project name")
@click.option('--amqp', default='amqp://localhost:5672', help="URL to the AMQP/RabbitMQ server")
@click.option('--influxdb', default='http://localhost:8086/bob', help="URL to the InfluxDB server")
@click.pass_context
def cli(ctx, log_file, log_level, project, amqp, influxdb):
    """Bas OBserve (BOb)."""
    config.setup_logging(level=log_level, logfile=log_file)
    log = logging.getLogger('CLI')  # re initiate logger
    ctx.obj['LOG'] = log

    if not project:
        log.error("Project name not specified!")
        ctx.exit()
    log.info(f"Started Bas OBserve with project {project}")

    ctx.obj['CONF'] = config.Config(project_name=project, amqp_url=amqp, influxdb_url=influxdb)


@cli.command('simulate', short_help="simulates agents by injecting packets from a log file")
@click.argument('dump')
@click.option('-f', '--dump-format', default='old', type=click.Choice(['old', 'new']))
@click.option('-a', '--agent', nargs=3, type=(str, int, int), multiple=True,
              help="defines an agent filter with <AGENT_NAME ADDR_FILTER ADDR_FILTER_MASK>")
@click.option('--length', type=int, default=10,
              help="Length of a window in seconds")
@click.option('--limit', type=int, default=0,
              help="Maximum amount of KNX packets to parse")
@click.option('--start', type=datetime, default=None,
              help="Timestamp where to start parsing the log")
@click.option('--end', type=datetime, default=None,
              help="Timestamp where to stop parsing the log")
@click.pass_context
def simulate(ctx, dump, dump_format, agent, length, limit, start, end):
    log = ctx.obj['LOG']
    agent_filter = {}
    for a in agent:
        if a[1] == 0 and a[2] == 0:
            mask = None  # None mask means, that every traffic matches
        else:
            mask = knx.bitmask.Bitmask(a[1], a[2])
        agent_filter[mask] = a[0]
        log.info(f"Defined agent {a[0]} with {mask}")

    agent_set = set(agent_filter.values())
    log.info(f"{len(agent_set)} agents defined: {', '.join(agent_set)}")

    agent = SimulatedAgent(
        ctx.obj['CONF'],
        dump,
        log_format=dump_format,
        agent_filter=agent_filter,
        window_length=timedelta(seconds=length) if length > 0 else None,
        start=start,
        end=end,
        limit=limit
    )
    agent.run()


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
@click.option('-m', '--model', help="Path to the trained model")
@click.pass_context
def analyse_addr(ctx, model):
    analyser = AddrAnalyser(ctx.obj['CONF'], model)
    analyser.analyse()


@analyse.command('entropy', short_help="start entropy estimation")
@click.option('-m', '--model', help="Path to the trained model")
@click.pass_context
def analyse_entropy(ctx, model):
    analyser = EntropyAnalyser(ctx.obj['CONF'], model)
    analyser.analyse()


@analyse.command('lof', short_help="start local outlier factor observation")
@click.option('-m', '--model', help="Path to the trained model")
@click.pass_context
def analyse_lof(ctx, model):
    analyser = LofAnalyser(ctx.obj['CONF'], model)
    analyser.analyse()


@analyse.command('svm', short_help="start SVM observation")
@click.option('-m', '--model', help="Path to the trained model")
@click.pass_context
def analyse_svm(ctx, model):
    analyser = SvmAnalyser(ctx.obj['CONF'], model)
    analyser.analyse()


# -----------------------------------------------------------------------------

@cli.group(short_help="trains one of the observation modules from InfluxDB")
@click.pass_context
def train(ctx):
    pass


@train.command('addr', short_help="gathers an address lookup table of a all address that have communicated")
@click.option('--start', help="Start date for the training data")
@click.option('--end', default=None, help="End date for the training data")
@click.option('-m', '--model', help="Path to the outputed model")
@click.pass_context
def tain_addr(ctx, start, end, model):
    analyser = AddrAnalyser(ctx.obj['CONF'], model)
    start = misc.parse_datetime(start)
    end = misc.parse_datetime(end)
    analyser.train(start, end)


@train.command('entropy', short_help="determines base line for entropy calculation")
@click.option('--start', help="Start date for the training data")
@click.option('--end', default=None, help="End date for the training data")
@click.option('-m', '--model', help="Path to the outputed model")
@click.pass_context
def train_entropy(ctx, start, end, model):
    analyser = EntropyAnalyser(ctx.obj['CONF'], model)
    start = misc.parse_datetime(start)
    end = misc.parse_datetime(end)
    analyser.train(start, end)


@train.command('lof', short_help="trains the base model for the local outlier factor")
@click.option('--start', help="Start date for the training data")
@click.option('--end', default=None, help="End date for the training data")
@click.option('-m', '--model', help="Path to the outputed model")
@click.pass_context
def train_lof(ctx, start, end, model):
    analyser = LofAnalyser(ctx.obj['CONF'], model)
    start = misc.parse_datetime(start)
    end = misc.parse_datetime(end)
    analyser.train(start, end)


@train.command('svm', short_help="trains a RBF Support Vector Machine")
@click.option('--start', help="Start date for the training data")
@click.option('--end', default=None, help="End date for the training data")
@click.option('-m', '--model', help="Path to the outputed model")
@click.pass_context
def train_svm(ctx, start, end, model):
    analyser = SvmAnalyser(ctx.obj['CONF'], model)
    start = misc.parse_datetime(start)
    end = misc.parse_datetime(end)
    analyser.train(start, end)


@train.command('arm')
@click.pass_context
def train_arm(ctx):
    pass


@train.command('leg')
@click.pass_context
def train_leg(ctc):
    pass


def run_cli():
    cli(auto_envvar_prefix='BOB', obj={})
