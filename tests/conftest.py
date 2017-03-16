import hashlib
import hmac
import json
import os
from collections import namedtuple
from datetime import datetime
from io import BytesIO

import pytest
import yaml
from aiohttp.web import Application, Response, json_response
from aiopg.sa import create_engine as aio_create_engine
from PIL import Image
from sqlalchemy import create_engine as sa_create_engine

from tcsocket.app.main import create_app
from tcsocket.app.management import psycopg2_cursor
from tcsocket.app.models import Base, sa_companies
from tcsocket.app.settings import load_settings, pg_dsn

DB = {
    'name': 'socket_test',
    'user': 'postgres',
    'password': os.getenv('APP_DATABASE_PASSWORD'),
    'host': 'localhost',
    'port': 5432,
}

MASTER_KEY = 'this is the master key'


async def test_image_view(request):
    image = Image.new('RGB', (2000, 1200), (50, 100, 150))
    stream = BytesIO()
    image.save(stream, format='JPEG', optimize=True)
    request.app['request_log'].append('test_image')
    return Response(body=stream.getvalue(), content_type='image/jpeg')


async def contractor_list_view(request):
    data = {
        1: {
            'count': 2,
            'next': f'{request.app["server_name"]}/api/contractors/?page=2',
            'previous': None,
            'results': [
                {
                    'id': 22,
                    'first_name': 'James',
                    'last_name': 'Higgins',
                    'town': 'London',
                    'country': 'United Kingdom',
                }
            ]
        },
        2: {
            'count': 2,
            'next': None,
            'previous': f'{request.app["server_name"]}/api/contractors/?page=1',
            'results': [
                {
                    'id': 23,
                    'last_name': 'Person 2',
                }
            ]
        },
    }
    page = int(request.GET.get('page', 1))
    request.app['request_log'].append('contractor_list')
    return json_response(data[page])


async def enquiry_options_view(request):
    request.app['request_log'].append('enquiry_options')
    return json_response({
        'name': 'Enquiries',
        'description': 'API endpoint for creating Enquiries.',
        'renders': [
            'application/json',
            'text/html'
        ],
        'parses': [
            'application/json'
        ],
        'actions': {
            'POST': {
                'client_name': {
                    'type': 'string',
                    'required': True,
                    'read_only': False,
                    'label': 'Name',
                    'max_length': 255,
                    'sort_index': 10,
                },
                'client_email': {
                    'type': 'email',
                    'required': False,
                    'read_only': False,
                    'label': 'Email',
                    'max_length': 255,
                    'sort_index': 20,
                },
                'client_phone': {
                    'type': 'string',
                    'required': False,
                    'read_only': False,
                    'label': 'Phone number',
                    'max_length': 255,
                    'sort_index': 30,
                },
                'service_recipient_name': {
                    'type': 'string',
                    'required': False,
                    'read_only': False,
                    'label': 'Student name',
                    'max_length': 255,
                    'sort_index': 40,
                },
                'attributes': {
                    'type': 'nested object',
                    'required': False,
                    'read_only': False,
                    'label': 'Attributes'
                },
                'contractor': {
                    'type': 'field',
                    'required': False,
                    'read_only': False,
                    'label': 'Tutor',
                    'sort_index': 50,
                },
                'subject': {
                    'type': 'field',
                    'required': False,
                    'read_only': False,
                    'label': 'Subject',
                    'sort_index': 60,
                },
                'qual_level': {
                    'type': 'field',
                    'required': False,
                    'read_only': False,
                    'label': 'Qualification Level',
                    'sort_index': 70,
                },
                'user_agent': {
                    'type': 'string',
                    'required': False,
                    'read_only': False,
                    'label': 'Browser User-Agent',
                    'max_length': 255,
                    'sort_index': 80,
                },
                'ip_address': {
                    'type': 'string',
                    'required': False,
                    'read_only': False,
                    'label': 'IP Address',
                    'sort_index': 90,
                },
                'http_referrer': {
                    'type': 'url',
                    'required': False,
                    'read_only': False,
                    'label': 'Referrer',
                    'max_length': 200,
                    'sort_index': 100,
                }
            }
        }
    })


async def enquiry_post_view(request):
    json_obj = await request.json()
    request.app['request_log'].append(('enquiry_post', json_obj))
    return json_response({'status': 'enquiry submitted, no-op'})


async def grecaptcha_post_view(request):
    data = await request.post()
    request.app['request_log'].append(('grecaptcha_post', dict(data)))
    if 'good' in data['response']:
        d = {
            'success': True,
            'challenge_ts': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'hostname': request.app['grecaptcha_host']
        }
    else:
        d = {'success': False, 'error-codes': ['invalid-input-response']}
    return json_response(d)


@pytest.fixture
def other_server(loop, test_server):
    app = Application(loop=loop)
    app.router.add_get('/_testing/image', test_image_view)
    app.router.add_get('/api/contractors/', contractor_list_view)
    app.router.add_route('OPTIONS', '/api/enquiry/', enquiry_options_view)
    app.router.add_post('/api/enquiry/', enquiry_post_view)
    app.router.add_post('/grecaptcha', grecaptcha_post_view)
    app.update(
        request_log=[],
        grecaptcha_host='www.example.com',
    )
    server = loop.run_until_complete(test_server(app))
    app['server_name'] = f'http://localhost:{server.port}'
    return server


@pytest.fixture
def image_download_url(other_server):
    return f'http://localhost:{other_server.port}/_testing/image'


@pytest.fixture
def settings(tmpdir, other_server):
    settings = {
        'database': DB,
        'redis': {
          'host': 'localhost',
          'port': 6379,
          'password': None,
          'database': 7,
        },
        'master_key': MASTER_KEY,
        'grecaptcha_secret': 'X' * 30,
        'grecaptcha_url': f'http://localhost:{other_server.port}/grecaptcha',
        'root_url': 'https://socket.tutorcruncher.com',
        'media_dir': str(tmpdir / 'media'),
        'media_url': 'https://socket.tutorcruncher.com/media',
        'tc_api_root': f'http://localhost:{other_server.port}/api'
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

    def acquire(self):
        return self

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

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
        app['worker']._concurrency_enabled = False
        await app['worker'].startup()
        app['worker'].pg_engine = app['pg_engine']
        redis_pool = await app['worker'].get_redis_pool()
        async with redis_pool.get() as redis:
            await redis.flushdb()

    app = create_app(loop, settings=settings)
    app.on_startup.append(modify_startup)
    return loop.run_until_complete(test_client(app))


@pytest.fixture
def company(loop, db_conn):
    public_key = 'thepublickey'
    private_key = 'theprivatekey'
    coro = db_conn.execute(
        sa_companies
        .insert()
        .values(name='foobar', public_key=public_key, private_key=private_key, domain='example.com')
    )
    loop.run_until_complete(coro)
    Company = namedtuple('Company', ['public_key', 'private_key'])
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
