import os

import pytest
from aiopg.sa import create_engine as aio_create_engine
from sqlalchemy import create_engine as sa_create_engine

from app.main import create_app, pg_dsn
from app.management import psycopg2_cursor
from app.models import Base


SETTINGS = {
    'database': {
        'name': 'socket_test',
        'user': 'postgres',
        'password': os.getenv('APP_DATABASE_PASSWORD'),
        'host': 'localhost',
        'port': 5432,
    },
    'redis': {
      'host': 'localhost',
      'port': 6379,
      'password': None,
      'database': 0,
    },
    'shared_secret': b'this is the secret key',
    'debug': True,
    'media': '/dev/null'
}
DB = SETTINGS['database']


@pytest.yield_fixture(scope='session')
def db():
    with psycopg2_cursor(**DB) as cur:
        cur.execute('DROP DATABASE IF EXISTS {name}'.format(**DB))
        cur.execute('CREATE DATABASE {name}'.format(**DB))

    engine = sa_create_engine(pg_dsn(DB))
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()

    with psycopg2_cursor(**DB) as cur:
        cur.execute('DROP DATABASE {name}'.format(**DB))


@pytest.yield_fixture
def db_conn(loop, db):
    engine = loop.run_until_complete(aio_create_engine(pg_dsn(DB), loop=loop))
    conn = loop.run_until_complete(engine.acquire())
    transaction = loop.run_until_complete(conn.begin())

    yield conn

    loop.run_until_complete(transaction.rollback())
    loop.run_until_complete(engine.release(conn))
    engine.close()
    loop.run_until_complete(engine.wait_closed())


class TestAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.fixture
def cli(loop, test_client, db_conn):
    """
    Create an app and client to interact with it

    The postgres pool's acquire method is changed to return a db connection which is in a transaction and is
    used by the test itself.
    """

    async def modify_startup(app):
        app['pg_engine'].acquire = lambda: TestAcquire(db_conn)
        app['image_worker'].concurrency_enabled = False

    app = create_app(loop, settings=SETTINGS)
    app.on_startup.append(modify_startup)
    return loop.run_until_complete(test_client(app))
