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
    'shared_secret': 'this is a secret',
    'debug': True,
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
def db_engine(loop, db):
    engine = loop.run_until_complete(aio_create_engine(pg_dsn(DB), loop=loop))

    yield engine

    engine.close()
    loop.run_until_complete(engine.wait_closed())


@pytest.yield_fixture
def db_conn(loop, db_engine):
    conn = loop.run_until_complete(db_engine.acquire())
    yield conn
    loop.run_until_complete(db_engine.release(conn))


@pytest.fixture
def cli(loop, test_client, db):
    app = create_app(loop, settings=SETTINGS)
    return loop.run_until_complete(test_client(app))
