"""
BAS Observe

Main entrypoint for the command line interface.
Called in __module__.py
"""

import click


@click.group()
def cli():
    """
    BAS Observe (BOb)
    """
    pass


@cli.command('simulate', short_help="simulates agents by injecting packets from a log file")
def simulate():
    pass


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
