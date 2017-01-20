import asyncio
import os
from functools import partial
from time import sleep

import click
from aiohttp import ClientSession
from arq import RunWorkerProcess
from gunicorn.app.base import BaseApplication

from .logs import logger
from .main import create_app
from .management import prepare_database

commands = []


def command(func):
    commands.append(func)
    return func


@command
def web(**kwargs):
    """
    Serve the application

    If the database doesn't already exist it will be created.
    """
    # TODO improve this with a real checker
    wait = 4
    logger.info('sleeping %ds to let database come up...', wait)
    sleep(wait)
    prepare_database(False, print_func=logger.info)
    # TODO logger.info("initialising application to check it's working...")

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


async def _check_web_coro(url):
    try:
        async with ClientSession() as session:
            async with session.get(url) as r:
                assert r.status == 200, f'response error {r.status} != 200'
    except (ValueError, AssertionError, OSError) as e:
        logger.error('web check error: %s: %s, url: "%s"', e.__class__.__name__, e, url)
        return 1
    else:
        logger.info('web check successful')


def _check_web():
    url = 'http://' + os.getenv('BIND', '127.0.0.1:8000')
    loop = asyncio.get_event_loop()
    exit_code = loop.run_until_complete(_check_web_coro(url))
    if exit_code:
        exit(exit_code)


def _check_worker():
    # TODO
    logger.warning('worker check not yet implemented')


@command
def check(**kwargs):
    """
    Check the application is running correctly, what this does depends on the CHECK environment variable
    """
    check_mode = os.getenv('CHECK')
    if check_mode == 'web':
        _check_web()
    elif check_mode == 'worker':
        _check_worker()
    else:
        raise ValueError('to use this the "CHECK" environment variable should be set to "web" or "worker"')


@command
def worker(**kwargs):
    """
    Run the worker
    """
    RunWorkerProcess('app/worker.py', 'Worker')


@command
def resetdb(*, no_input, **kwargs):
    """
    create a database and run migrations, optionally deleting an existing database.
    """
    delete = no_input or partial(click.confirm, 'Are you sure you want to delete the database and recreate it?')
    prepare_database(delete, print_func=logger.info)
