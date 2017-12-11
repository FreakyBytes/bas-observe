"""
BAS Observe

Main entrypoint for the command line interface.
Called in __module__.py
"""

import click


@click.group()
def cli():
    """
    Bas OBserve (bob)
    """
    pass


@cli.command('simulate', short_help="simulates agents by injecting packets from a log file")
def simulate():
    pass


@cli.command('log', short_help="logs packages and observation results to InfluxDB")
def log():
    pass


# -----------------------------------------------------------------------------


@cli.group(short_help="starts one of the observation modules")
def observe():
    pass


@observe.command('addr', short_help="start address lookup observation")
def observe_addr():
    pass


@observe.command('entropy', short_help="start entropy estimation")
def observe_entropy():
    pass


@observe.command('lof', short_help="start local outlier factor observation")
def observe_lof():
    pass


# -----------------------------------------------------------------------------

@cli.group(short_help="trains one of the observation modules from InfluxDB")
def train():
    pass


@train.command('addr', short_help="gathers an address lookup table of a all address that have communicated")
def tain_addr():
    pass


@train.command('entropy', short_help="determines base line from entropy calculation")
def train_entropy():
    pass


@train.command('lof')
def train_lof():
    pass
