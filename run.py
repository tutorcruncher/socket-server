#!/usr/bin/env python3.6
import asyncio
import os
from arq import RunWorkerProcess
from functools import partial
from time import sleep

import click
from gunicorn.app.base import BaseApplication

from app.logs import logger, setup_logging
from app.main import create_app
from app.management import prepare_database


@click.group()
def cli():
    """
    Run TutorCruncher socket
    """
    pass


@cli.command()
@click.option('-v', '--verbose', is_flag=True)
def web(verbose):
    """
    Serve the application

    If the database doesn't already exist it will be created.
    """
    setup_logging(verbose)
    wait = 2
    logger.info('sleeping %ds to let database come up...', wait)
    sleep(wait)
    prepare_database(False, print_func=logger.info)
    logger.info("initialising application to check it's working...")

    config = dict(
        worker_class='aiohttp.worker.GunicornWebWorker',
        bind=os.getenv('BIND', '127.0.0.1:8000'),
        workers=int(os.getenv('WEB_CONCURRENCY', '1')),
        max_requests=5000,
        max_requests_jitter=500,
    )

    class Application(BaseApplication):
        def load_config(self):
            for k, v in config.items():
                self.cfg.set(k, v)

        def load(self):
            loop = asyncio.get_event_loop()
            return create_app(loop)

    Application().run()


@cli.command()
@click.option('-v', '--verbose', is_flag=True)
def worker(verbose):
    """
    Run the worker
    """
    setup_logging(verbose)

    RunWorkerProcess('app/worker.py', 'Worker')


@cli.command()
@click.option('--no-input', is_flag=True)
@click.option('-v', '--verbose', is_flag=True)
def resetdb(no_input, verbose):
    """
    create a database and run migrations, optionally deleting an existing database.
    """
    setup_logging(verbose)
    confirm = no_input or partial(click.confirm, 'Are you sure you want to delete the database and recreate it?')
    prepare_database(confirm, print_func=logger.info)


if __name__ == '__main__':
    cli()
