import hashlib
import hmac
import json
import os
from collections import namedtuple
from io import BytesIO

import pytest
import yaml
from aiohttp.web import Application, Response
from aiopg.sa import create_engine as aio_create_engine
from PIL import Image
from sqlalchemy import create_engine as sa_create_engine

from tcsocket.app.main import create_app, pg_dsn
from tcsocket.app.management import psycopg2_cursor
from tcsocket.app.models import Base, sa_companies
from tcsocket.app.settings import load_settings

DB = {
    'name': 'socket_test',
    'user': 'postgres',
    'password': os.getenv('APP_DATABASE_PASSWORD'),
    'host': 'localhost',
    'port': 5432,
}

MASTER_KEY = 'this is the master key'


@pytest.fixture
def settings(tmpdir):
    settings = {
        'database': DB,
        'redis': {
          'host': 'localhost',
          'port': 6379,
          'password': None,
          'database': 0,
        },
        'master_key': MASTER_KEY,
        'root_url': 'http://socket.tutorcruncher.com',
        'media_dir': str(tmpdir / 'media'),
        'media_url': 'http://socket.tutorcruncher.com/media',
    }
    s_file = tmpdir / 'settings.yaml'
    s_file.write(yaml.dump(settings, default_flow_style=False))
    return load_settings(s_file, env_prefix='TESTING_APP_')


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


class TestEngine:
    def __init__(self, conn):
        self._conn = conn

    async def _acquire(self):
        return self._conn

    async def release(self, conn):
        pass

    def close(self):
        pass

    async def wait_closed(self):
        pass


@pytest.fixture
def cli(loop, test_client, db_conn, settings):
    """
    Create an app and client to interact with it

    The postgres pool's acquire method is changed to return a db connection which is in a transaction and is
    used by the test itself.
    """

    async def modify_startup(app):
        app['pg_engine'] = TestEngine(db_conn)
        app['request_worker']._concurrency_enabled = False

    app = create_app(loop, settings=settings)
    app.on_startup.append(modify_startup)
    return loop.run_until_complete(test_client(app))


async def test_image(request):
    image = Image.new('RGB', (2000, 1200), (50, 100, 150))

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
    public_key = 'thepublickey'
    private_key = 'theprivatekey'
    coro = db_conn.execute(
        sa_companies
        .insert()
        .values(name='foobar', public_key=public_key, private_key=private_key)
    )
    loop.run_until_complete(coro)
    Company = namedtuple('Point', ['public_key', 'private_key'])
    return Company(public_key, private_key)


async def signed_post(cli, url, *, signing_key_=MASTER_KEY, **data):
    payload = json.dumps(data)
    b_payload = payload.encode()
    m = hmac.new(signing_key_.encode(), b_payload, hashlib.sha256)
    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    return await cli.post(url, data=payload, headers=headers)
