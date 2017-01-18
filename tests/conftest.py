import hashlib
import hmac
import json
import os
from io import BytesIO

import pytest
from aiohttp.web import Application, Response
from aiopg.sa import create_engine as aio_create_engine
from PIL import Image
from sqlalchemy import create_engine as sa_create_engine

from app.main import create_app, pg_dsn
from app.management import psycopg2_cursor
from app.models import Base, sa_companies

DB = {
    'name': 'socket_test',
    'user': 'postgres',
    'password': os.getenv('APP_DATABASE_PASSWORD'),
    'host': 'localhost',
    'port': 5432,
}


@pytest.fixture
def settings(tmpdir):
    return {
        'database': DB,
        'redis': {
          'host': 'localhost',
          'port': 6379,
          'password': None,
          'database': 0,
        },
        'shared_secret': b'this is the secret key',
        'debug': True,
        'media': str(tmpdir)
    }


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
def cli(loop, test_client, db_conn, settings):
    """
    Create an app and client to interact with it

    The postgres pool's acquire method is changed to return a db connection which is in a transaction and is
    used by the test itself.
    """

    async def modify_startup(app):
        app['pg_engine'].acquire = lambda: TestAcquire(db_conn)
        app['image_worker']._concurrency_enabled = False

    app = create_app(loop, settings=settings)
    app.on_startup.append(modify_startup)
    return loop.run_until_complete(test_client(app))


async def test_image(request):
    image = Image.new('RGB', (1200, 600), (50, 100, 150))

    stream = BytesIO()
    image.save(stream, format='JPEG', optimize=True)
    return Response(body=stream.getvalue(), content_type='image/jpeg')


@pytest.fixture
def image_download_url(loop, test_server):
    app = Application(loop=loop)
    app.router.add_get('/_testing/image', test_image)
    server = loop.run_until_complete(test_server(app))

    return f'http://localhost:{server.port}/_testing/image'


@pytest.fixture
def company(loop, db_conn):
    key = 'thekey'
    coro = db_conn.execute(
        sa_companies
        .insert()
        .values(name='foobar', key=key)
    )
    loop.run_until_complete(coro)
    return key


async def signed_post(cli, url, **data):
    payload = json.dumps(data)
    b_payload = payload.encode()
    m = hmac.new(b'this is the secret key', b_payload, hashlib.sha256)

    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    return await cli.post(url, data=payload, headers=headers)
