import hashlib
import hmac
import json
from collections import namedtuple
from datetime import datetime
from io import BytesIO
from itertools import product
from time import time

import pytest
from aiohttp.web import Application, Response, json_response
from aiopg.sa import create_engine as aio_create_engine
from PIL import Image
from sqlalchemy import create_engine as sa_create_engine
from sqlalchemy import select
from sqlalchemy.sql.functions import count as count_func

from tcsocket.app.main import create_app
from tcsocket.app.management import populate_db, psycopg2_cursor
from tcsocket.app.models import sa_companies, sa_con_skills, sa_qual_levels, sa_subjects
from tcsocket.app.settings import Settings

DB_NAME = 'socket_test'
MASTER_KEY = 'this is the master key'


async def test_image_view(request):
    image = Image.new('RGB', (2000, 1200), (50, 100, 150))
    stream = BytesIO()
    image.save(stream, format='JPEG', optimize=True)
    request.app['request_log'].append('test_image')
    return Response(body=stream.getvalue(), content_type='image/jpeg')


async def contractor_list_view(request):
    request.app['request_log'].append(('contractor_list', request.GET.get('page')))
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
    return json_response(data[page])


async def enquiry_options_view(request):
    request.app['request_log'].append('enquiry_options')
    extra_attributes = request.app.get('extra_attributes')
    if extra_attributes == 'default':
        attribute_children = {
            'tell-us-about-yourself': {
                'type': 'string',
                'required': True,
                'read_only': True,
                'label': 'Tell us about yourself',
                'help_text': 'whatever',
                'max_length': 2047,
                'sort_index': 1000
            },
            'how-did-you-hear-about-us': {
                'type': 'choice',
                'required': False,
                'read_only': True,
                'label': '...',
                'choices': [
                    {'value': '', 'display_name': ''},
                    {'value': 'foo', 'display_name': 'Foo'},
                    {'value': 'bar', 'display_name': 'Bar'},
                ],
                'sort_index': 1001
            },
            'date-of-birth': {
                'type': 'date',
                'required': False,
                'read_only': True,
                'label': 'Date of Birth',
                'help_text': 'Date your child was born',
                'sort_index': 1003
            }
        }
    elif extra_attributes == 'datetime':
        attribute_children = {
            'date-field': {
                'type': 'date',
                'required': True,
                'read_only': True,
                'label': 'Foobar date',
                'help_text': 'xxx',
                'sort_index': 1000
            },
            'datetime-field': {
                'type': 'datetime',
                'required': True,
                'read_only': True,
                'label': 'Foobar datetime',
                'help_text': 'xxx',
                'sort_index': 1001
            }
        }
    else:
        attribute_children = {}
    return json_response({
        'name': 'Enquiries',
        '_': 'unused fields missing...',
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
                    'label': 'Attributes',
                    'children': attribute_children,
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
    referrer = json_obj.get('http_referrer') or ''
    if 'snap' in referrer:
        raise RuntimeError('enquiry_post_view snap')
    request.app['request_log'].append(('enquiry_post', json_obj))
    return json_response({'status': 'enquiry submitted, no-op'}, status=400 if '400' in referrer else 200)


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


async def geocoding_view(request):
    address = request.GET.get('address')
    if address == 'SW1W 0EN':
        loc = {
            'results': [
                {
                    'address_components': None,
                    'formatted_address': 'Lower Grosvenor Pl, Westminster, London SW1W 0EN, UK',
                    'geometry': {
                        'bounds': None,
                        'location': {
                            'lat': 51.4980603,
                            'lng': -0.14505,
                        },
                        'location_type': 'APPROXIMATE',
                        'viewport': None,
                    },
                    'types': ['postal_code'],
                },
            ],
            'status': 'OK',
        }
    else:
        loc = {
            'results': [],
            'status': 'INVALID_REQUEST',
        }
    return json_response(loc)


@pytest.fixture
def other_server(loop, test_server):
    app = Application(loop=loop)
    app.router.add_get('/_testing/image', test_image_view)
    app.router.add_get('/api/contractors/', contractor_list_view)
    app.router.add_route('OPTIONS', '/api/enquiry/', enquiry_options_view)
    app.router.add_post('/api/enquiry/', enquiry_post_view)
    app.router.add_post('/grecaptcha', grecaptcha_post_view)
    app.router.add_get('/geocode', geocoding_view)
    app.update(
        request_log=[],
        grecaptcha_host='example.com',
    )
    server = loop.run_until_complete(test_server(app))
    app['server_name'] = f'http://localhost:{server.port}'
    return server


@pytest.fixture
def image_download_url(other_server):
    return f'http://localhost:{other_server.port}/_testing/image'


@pytest.fixture
def settings(tmpdir, other_server):
    return Settings(
        pg_name='socket_test',
        redis_database=7,
        master_key=MASTER_KEY,
        grecaptcha_secret='X' * 30,
        media_dir=str(tmpdir / 'media'),
        root_url='https://socket.tutorcruncher.com',
        media_url='https://socket.tutorcruncher.com/media',
        grecaptcha_url=f'http://localhost:{other_server.port}/grecaptcha',
        tc_api_root=f'http://localhost:{other_server.port}/api',
        geocoding_url=f'http://localhost:{other_server.port}/geocode',
    )


@pytest.yield_fixture(scope='session')
def db():
    settings_: Settings = Settings(pg_name=DB_NAME)
    with psycopg2_cursor(settings_) as cur:
        cur.execute(f'DROP DATABASE IF EXISTS {settings_.pg_name}')
        cur.execute(f'CREATE DATABASE {settings_.pg_name}')

    engine = sa_create_engine(settings_.pg_dsn)
    populate_db(engine)
    yield engine
    engine.dispose()


@pytest.yield_fixture
def db_conn(loop, db, settings):
    engine = loop.run_until_complete(aio_create_engine(settings.pg_dsn, loop=loop))
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
        redis = await app['worker'].get_redis()
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
        .values(name='foobar', public_key=public_key, private_key=private_key, domains=['example.com'])
        .returning(sa_companies.c.id)
    )
    v = loop.run_until_complete(coro)
    company_id = loop.run_until_complete(v.first()).id
    Company = namedtuple('Company', ['public_key', 'private_key', 'id'])
    return Company(public_key, private_key, company_id)


async def signed_post(cli, url_, *, signing_key_=MASTER_KEY, **data):
    data.setdefault('_request_time', int(time()))
    payload = json.dumps(data)
    b_payload = payload.encode()
    m = hmac.new(signing_key_.encode(), b_payload, hashlib.sha256)
    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    return await cli.post(url_, data=payload, headers=headers)


async def count(db_conn, sa_table):
    cur = await db_conn.execute(select([count_func()]).select_from(sa_table))
    return (await cur.first())[0]


async def select_set(db_conn, *fields, select_from=None):
    q = select(fields)
    if select_from is not None:
        q = q.select_from(select_from)
    return {tuple(cs.values()) async for cs in await db_conn.execute(q)}


async def get(db_conn, model, *where):
    v = list(await db_conn.execute(
        select([c for c in model.c])
        .where(*where)
    ))
    if len(v) != 1:
        raise RuntimeError(f'get got wrong number of results: {len(v)} != 1, model: {model}')
    return dict(v[0])


async def create_con_skills(db_conn, *con_ids):
    await db_conn.execute(
        sa_subjects
        .insert()
        .values([
            {'id': 1, 'name': 'Mathematics', 'category': 'Maths'},
            {'id': 2, 'name': 'Language', 'category': 'English'},
            {'id': 3, 'name': 'Literature', 'category': 'English'},
        ])
    )
    await db_conn.execute(
        sa_qual_levels
        .insert()
        .values([
            {'id': 11, 'name': 'GCSE', 'ranking': 16},
            {'id': 12, 'name': 'A Level', 'ranking': 18},
            {'id': 13, 'name': 'Degree', 'ranking': 21},
        ])
    )
    skill_ids = [(1, 11), (2, 12)]

    await db_conn.execute(
        sa_con_skills
        .insert()
        .values(
            [{'contractor': con_id, 'subject': s[0], 'qual_level': s[1]} for con_id, s in product(con_ids, skill_ids)]
        )
    )
