#!/usr/bin/env python3.6
import asyncio
import logging
import os
from functools import partial

import click
from aiohttp import ClientSession
from arq import Worker, run_worker
from arq.connections import RedisSettings
from gunicorn.app.base import BaseApplication

from tcsocket.app.logs import setup_logging
from tcsocket.app.main import create_app
from tcsocket.app.management import prepare_database, run_patch
from tcsocket.app.settings import Settings
from tcsocket.app.worker import WorkerSettings

logger = logging.getLogger('socket.run')


@click.group()
@click.option('-v', '--verbose', is_flag=True)
def cli(verbose):
    """
    Run TutorCruncher socket
    """
    setup_logging(verbose)


async def _check_port_open(host, port, loop):
    steps, delay = 100, 0.1
    for i in range(steps):
        try:
            await loop.create_connection(lambda: asyncio.Protocol(), host=host, port=port)
        except OSError:
            await asyncio.sleep(delay, loop=loop)
        else:
            logger.info('Connected successfully to %s:%s after %0.2fs', host, port, delay * i)
            return
    raise RuntimeError(f'Unable to connect to {host}:{port} after {steps * delay}s')


def check_services_ready():
    settings = Settings()
    loop = asyncio.get_event_loop()
    coros = [
        _check_port_open(settings.pg_host, settings.pg_port, loop),
        _check_port_open(settings.redis_host, settings.redis_port, loop),
    ]
    loop.run_until_complete(asyncio.gather(*coros, loop=loop))


def check_app():
    loop = asyncio.get_event_loop()
    logger.info("initialising aiohttp app to check it's working...")
    app = create_app(loop)
    app.freeze()
    loop.run_until_complete(app.startup())
    loop.run_until_complete(app.cleanup())
    del app
    logger.info('app started and stopped successfully, apparently configured correctly')


@cli.command()
def web():
    """
    Serve the application

    If the database doesn't already exist it will be created.
    """
    logger.info('waiting for postgres and redis to come up...')
    check_services_ready()

    logger.info('preparing the database...')
    prepare_database(False)

    check_app()

    config = dict(
        worker_class='aiohttp.worker.GunicornUVLoopWebWorker',
        bind=os.getenv('BIND', '127.0.0.1:8000'),
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

    logger.info('starting gunicorn...')
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
    exit(Worker.check_health())


@cli.command()
def check():
    """
    Check the application is running correctly, what this does depends on the CHECK environment variable
    """
    check_mode = os.getenv('CHECK')
    if check_mode == 'web':
        _check_web()
    elif check_mode == 'worker':
        _check_worker()
    else:
        raise ValueError(f'"CHECK" environment variable should be set to "web" or "worker" not "{check_mode}"')


@cli.command()
def worker():
    """
    Run the worker
    """
    logger.info('waiting for redis to come up...')
    check_services_ready()
    run_worker(WorkerSettings, ctx={'settings': RedisSettings()})


@cli.command()
@click.option('--no-input', is_flag=True)
def resetdb(no_input):
    """
    create a database and run migrations, optionally deleting an existing database.
    """
    delete = no_input or partial(click.confirm, 'Are you sure you want to delete the database and recreate it?')
    prepare_database(delete)


EXEC_LINES = [
    'import asyncio, os, re, sys',
    'from datetime import datetime, timedelta, timezone',
    'from pprint import pprint as pp',
    '',
    'from sqlalchemy import create_engine',
    'from sqlalchemy import func, select, update',
    'from app.settings import Settings',
    'from app.models import sa_companies, sa_contractors, sa_subjects, sa_qual_levels, sa_con_skills',
    '',
    'loop = asyncio.get_event_loop()',
    'await_ = loop.run_until_complete',
    'settings = Settings()',
    'engine = create_engine(settings.pg_dsn)',
    'conn = engine.connect()',
]
EXEC_LINES += (
    ['print("\\n    Python {v.major}.{v.minor}.{v.micro}\\n".format(v=sys.version_info))'] +
    [f'print("    {l}")' for l in EXEC_LINES]
)


@cli.command()
def shell():
    """
    ipython shell
    """
    from IPython import start_ipython
    from IPython.terminal.ipapp import load_default_config
    c = load_default_config()

    c.TerminalIPythonApp.display_banner = False
    c.TerminalInteractiveShell.confirm_exit = False
    c.InteractiveShellApp.exec_lines = EXEC_LINES
    start_ipython(argv=(), config=c)


@cli.command()
@click.option('--live', is_flag=True)
@click.argument('patch', required=False)
def patch(live, patch):
    """
    Run patch script
    """
    run_patch(live, patch)


if __name__ == '__main__':
    cli()
