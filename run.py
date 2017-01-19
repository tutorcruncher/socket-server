#!/usr/bin/env python3.6
import click

from app.logs import logger, setup_logging
from app.management import commands


@click.command()
@click.argument('command', type=click.Choice([c.__name__ for c in commands]))
@click.option('--no-input', is_flag=True)
@click.option('-v', '--verbose', is_flag=True)
def cli(*, command, no_input, verbose):
    """
    Run TutorCruncher socket
    """
    setup_logging(verbose)

    command_lookup = {c.__name__: c for c in commands}

    func = command_lookup[command]
    logger.info('running %s...', func.__name__)
    func(verbose=verbose, no_input=no_input)


if __name__ == '__main__':
    cli()
