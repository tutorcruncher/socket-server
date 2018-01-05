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
from .settings import Settings

commands = []


def command(func):
    commands.append(func)
    return func


def lenient_connection(settings: Settings, retries=5):
    try:
        return psycopg2.connect(
            password=settings.pg_password,
            host=settings.pg_host,
            port=settings.pg_port,
            user=settings.pg_user,
        )
    except psycopg2.Error as e:
        if retries <= 0:
            raise
        else:
            logger.warning('%s: %s (%d retries remaining)', e.__class__.__name__, e, retries)
            sleep(1)
            return lenient_connection(settings, retries=retries - 1)


@contextmanager
def psycopg2_cursor(settings):
    conn = lenient_connection(settings)
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
    settings = Settings()

    with psycopg2_cursor(settings) as cur:
        cur.execute('SELECT EXISTS (SELECT datname FROM pg_catalog.pg_database WHERE datname=%s)', (settings.pg_name,))
        already_exists = bool(cur.fetchone()[0])
        if already_exists:
            if callable(delete_existing):
                _delete_existing = delete_existing()
            else:
                _delete_existing = bool(delete_existing)
            if not _delete_existing:
                print_func(f'database "{settings.pg_name}" already exists, not recreating it')
                return False
            else:
                print_func(f'dropping existing connections to "{settings.pg_name}"...')
                cur.execute(DROP_CONNECTIONS, (settings.pg_name,))
                print_func(f'dropping database "{settings.pg_name}" as it already exists...')
                cur.execute(f'DROP DATABASE {settings.pg_name}')
        else:
            print_func(f'database "{settings.pg_name}" does not yet exist')

        print_func(f'creating database "{settings.pg_name}"...')
        cur.execute(f'CREATE DATABASE {settings.pg_name}')

    engine = create_engine(settings.pg_dsn)
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


patches = []


def patch(func):
    patches.append(func)
    return func


def run_patch(live, patch_name):
    if patch_name is None:
        print('available patches:\n{}'.format(
            '\n'.join('  {}: {}'.format(p.__name__, p.__doc__.strip('\n ')) for p in patches)
        ))
        return
    patch_lookup = {p.__name__: p for p in patches}
    try:
        patch_func = patch_lookup[patch_name]
    except KeyError:
        raise RuntimeError(f'patch {patch_name} not found in patches: {[p.__name__ for p in patches]}')

    print(f'running patch {patch_name} live {live}')
    settings = Settings()
    engine = create_engine(settings.pg_dsn)
    conn = engine.connect()
    trans = conn.begin()
    print('=' * 40)
    try:
        patch_func(conn=conn, settings=settings)
    except BaseException as e:
        print('=' * 40)
        trans.rollback()
        raise RuntimeError('error running patch, rolling back') from e
    else:
        print('=' * 40)
        if live:
            trans.commit()
            print('live, committed patch')
        else:
            print('not live, rolling back')
            trans.rollback()


@patch
def print_tables(conn, settings):
    """
    print names of all tables
    """
    result = conn.execute("""SELECT tablename FROM pg_catalog.pg_tables where schemaname='public'""")
    for row in result:
        print(row[0])


@patch
def add_labels(conn, settings):
    """
    add labels field to contractors
    """
    conn.execute('ALTER TABLE contractors ADD labels VARCHAR(255)[]')
    conn.execute("""
    CREATE INDEX ix_contractors_labels
      ON contractors
      USING btree (labels);""")
