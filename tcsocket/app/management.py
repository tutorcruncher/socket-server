import asyncio
import os
from contextlib import contextmanager
from functools import partial
from time import sleep
from typing import Union

import click
import psycopg2
from aiohttp import ClientSession
from arq import RunWorkerProcess
from gunicorn.app.base import BaseApplication
from sqlalchemy import create_engine

from .logs import logger
from .main import create_app
from .models import Base
from .settings import load_settings, pg_dsn

commands = []


def command(func):
    commands.append(func)
    return func


def lenient_connection(retries=5, **db_settings):
    try:
        return psycopg2.connect(
            password=db_settings['password'],
            host=db_settings['host'],
            port=db_settings['port'],
            user=db_settings['user'],
        )
    except psycopg2.Error as e:
        if retries <= 0:
            raise
        else:
            logger.warning('%s: %s (%d retries remaining)', e.__class__.__name__, e, retries)
            sleep(1)
            return lenient_connection(retries=retries - 1, **db_settings)


@contextmanager
def psycopg2_cursor(**db_settings):
    conn = lenient_connection(**db_settings)
    conn.autocommit = True
    cur = conn.cursor()

    yield cur

    cur.close()
    conn.close()


def populate_db(engine):
    engine.execute('CREATE EXTENSION IF NOT EXISTS cube')
    engine.execute('CREATE EXTENSION IF NOT EXISTS earthdistance')
    Base.metadata.create_all(engine)


DROP_CONNECTIONS = """\
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = %s AND pid <> pg_backend_pid();
"""


def prepare_database(delete_existing: Union[bool, callable], print_func=print) -> bool:
    """
    (Re)create a fresh database and run migrations.

    :param delete_existing: whether or not to drop an existing database if it exists
    :param print_func: function to use for printing, eg. could be set to `logger.info`
    :return: whether or not a database as (re)created
    """
    db = load_settings()['database']

    with psycopg2_cursor(**db) as cur:
        cur.execute('SELECT EXISTS (SELECT datname FROM pg_catalog.pg_database WHERE datname=%s)', (db['name'],))
        already_exists = bool(cur.fetchone()[0])
        if already_exists:
            if callable(delete_existing):
                _delete_existing = delete_existing()
            else:
                _delete_existing = bool(delete_existing)
            if not _delete_existing:
                print_func('database "{name}" already exists, not recreating it'.format(**db))
                return False
            else:
                print_func('dropping existing connections to "{name}"...'.format(**db))
                cur.execute(DROP_CONNECTIONS, (db['name'],))
                print_func('dropping database "{name}" as it already exists...'.format(**db))
                cur.execute('DROP DATABASE {name}'.format(**db))
        else:
            print_func('database "{name}" does not yet exist'.format(**db))

        print_func('creating database "{name}"...'.format(**db))
        cur.execute('CREATE DATABASE {name}'.format(**db))

    engine = create_engine(pg_dsn(db))
    print_func('creating tables from model definition...')
    populate_db(engine)
    engine.dispose()
    print_func('db and tables creation finished.')
    return True


@command
def web(**kwargs):
    """
    Serve the application

    If the database doesn't already exist it will be created.
    """
    wait = 4
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
