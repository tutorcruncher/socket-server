import hashlib
import hmac
import json
import os
from collections import namedtuple
from datetime import datetime
from io import BytesIO
from itertools import product
from time import time

import pytest
from aiohttp import ClientSession, ClientTimeout
from aiohttp.web import Application, Response, json_response
from aiopg.sa import create_engine as aio_create_engine
from aioredis import create_redis
from arq import Worker
from arq.connections import ArqRedis
from PIL import Image, ImageDraw
from sqlalchemy import create_engine as sa_create_engine, select
from sqlalchemy.sql.functions import count as count_func

from tcsocket.app.main import create_app
from tcsocket.app.management import populate_db, prepare_database
from tcsocket.app.models import sa_appointments, sa_companies, sa_con_skills, sa_qual_levels, sa_services, sa_subjects
from tcsocket.app.settings import Settings
from tcsocket.app.worker import WorkerSettings, startup

MASTER_KEY = 'this is the master key'
DB_DSN = 'postgresql://postgres@localhost:5432/socket_test'


async def test_image_view(request):
    image_format = request.query.get('format')
    stream = BytesIO()
    request.app['request_log'].append(('test_image', image_format))

    if image_format == 'RGBA':
        create_as, save_as = 'RGBA', 'PNG'
    elif image_format == 'P':
        create_as, save_as = 'RGBA', 'GIF'
    else:
        create_as, save_as = 'RGB', 'JPEG'

    image = Image.new(create_as, (2000, 1200), (50, 100, 150))
    ImageDraw.Draw(image).polygon([(0, 0), (image.width, 0), (image.width, 100), (0, 100)], fill=(128, 128, 128))
    kwargs = dict(format=save_as, optimize=True)
    if request.query.get('exif'):
        kwargs['exif'] = (
            b'Exif\x00\x00MM\x00*\x00\x00\x00\x08\x00\x01\x01\x12\x00'
            b'\x03\x00\x00\x00\x01\x00\x06\x00\x00\x00\x00\x00\x00'
        )
    image.save(stream, **kwargs)
    return Response(body=stream.getvalue(), content_type=f'image/{save_as.lower()}')


async def contractor_list_view(request):
    request.app['request_log'].append(('contractor_list', request.query.get('page')))
    data = {
        1: {
            'count': 2,
            'next': f'{request.app["extra"]["server_name"]}/api/public_contractors/?page=2',
            'previous': None,
            'results': [
                {
                    'id': 22,
                    'first_name': 'James',
                    'last_name': 'Higgins',
                    'town': 'London',
                    'country': 'United Kingdom',
                }
            ],
        },
        2: {
            'count': 2,
            'next': None,
            'previous': f'{request.app["extra"]["server_name"]}/api/public_contractors/?page=1',
            'results': [{'id': 23, 'last_name': 'Person 2'}],
        },
    }
    page = int(request.query.get('page', 1))
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
                'sort_index': 1000,
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
                'sort_index': 1001,
            },
            'date-of-birth': {
                'type': 'date',
                'required': False,
                'read_only': True,
                'label': 'Date of Birth',
                'help_text': 'Date your child was born',
                'sort_index': 1003,
            },
        }
    elif extra_attributes == 'datetime':
        attribute_children = {
            'date-field': {
                'type': 'date',
                'required': True,
                'read_only': True,
                'label': 'Foobar date',
                'help_text': 'xxx',
                'sort_index': 1000,
            },
            'datetime-field': {
                'type': 'datetime',
                'required': True,
                'read_only': True,
                'label': 'Foobar datetime',
                'help_text': 'xxx',
                'sort_index': 1001,
            },
        }
    elif extra_attributes == 'all_optional':
        attribute_children = {
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
                'sort_index': 1001,
            },
            'date-of-birth': {
                'type': 'date',
                'required': False,
                'read_only': True,
                'label': 'Date of Birth',
                'help_text': 'Date your child was born',
                'sort_index': 1003,
            },
        }
    else:
        attribute_children = {}
    return json_response(
        {
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
                    },
                }
            },
        }
    )


async def enquiry_post_view(request):
    json_obj = await request.json()
    referrer = json_obj.get('http_referrer') or ''
    if 'snap' in referrer:
        return Response(text='error', status=500)
    request.app['request_log'].append(('enquiry_post', json_obj))
    return json_response({'status': 'enquiry submitted, no-op'}, status=400 if '400' in referrer else 200)


async def booking_post_view(request):
    json_obj = await request.json()
    request.app['request_log'].append(('booking_post', json_obj))
    return json_response({'status': 'booking submitted, no-op'})


async def grecaptcha_post_view(request):
    data = await request.post()
    request.app['request_log'].append(('grecaptcha_post', dict(data)))
    if 'good' in data['response']:
        d = {
            'success': True,
            'challenge_ts': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'hostname': request.app['grecaptcha_host'],
        }
    else:
        d = {'success': False, 'error-codes': ['invalid-input-response']}
    return json_response(d)


async def geocoding_view(request):
    address = request.query.get('address')
    region = request.query.get('region')
    request.app['request_log'].append(('geocode', f'{address}|{region}'))
    status = 200
    if address == 'SW1W 0EN':
        loc = {
            'results': [
                {
                    'address_components': None,
                    'formatted_address': 'Lower Grosvenor Pl, Westminster, London SW1W 0EN, UK',
                    'geometry': {
                        'bounds': None,
                        'location': {'lat': 51.4980603, 'lng': -0.14505},
                        'location_type': 'APPROXIMATE',
                        'viewport': None,
                    },
                    'types': ['postal_code'],
                },
            ],
            'status': 'OK',
        }
    elif address == '500':
        status = 500
        loc = {
            'results': [],
            'status': 'error',
        }
    else:
        status = 400
        loc = {
            'results': [],
            'status': 'INVALID_REQUEST',
        }
    return json_response(loc, status=status)


@pytest.fixture(name='redis')
async def _fix_redis(settings):
    addr = settings.redis_settings.host, settings.redis_settings.port

    redis = await create_redis(addr, db=settings.redis_settings.database, encoding='utf8', commands_factory=ArqRedis)
    await redis.flushdb()

    yield redis

    redis.close()
    await redis.wait_closed()


@pytest.fixture(name='worker_ctx')
async def _fix_worker_ctx(redis, settings, db_conn):
    session = ClientSession(timeout=ClientTimeout(total=10))
    ctx = dict(settings=settings, pg_engine=MockEngine(db_conn), session=session, redis=redis)

    yield ctx

    await session.close()


@pytest.fixture(name='worker')
async def _fix_worker(redis, worker_ctx):
    worker = Worker(functions=WorkerSettings.functions, redis_pool=redis, burst=True, poll_delay=0.01, ctx=worker_ctx)

    yield worker

    # Sets the pool to use our settings RedisSettings instead of ArqRedis
    worker._pool = None
    await worker.close()


@pytest.fixture
def other_server(loop, aiohttp_server):
    app = Application()
    app.router.add_get('/_testing/image', test_image_view)
    app.router.add_get('/api/public_contractors/', contractor_list_view)
    app.router.add_route('OPTIONS', '/api/enquiry/', enquiry_options_view)
    app.router.add_post('/api/enquiry/', enquiry_post_view)
    app.router.add_post('/api/recipient_appointments/', booking_post_view)
    app.router.add_post('/grecaptcha', grecaptcha_post_view)
    app.router.add_get('/geocode', geocoding_view)
    app.update(
        request_log=[],
        grecaptcha_host='example.com',
        extra={},
    )
    server = loop.run_until_complete(aiohttp_server(app))
    app['extra']['server_name'] = f'http://localhost:{server.port}'
    return server


@pytest.fixture
def image_download_url(other_server):
    return f'http://localhost:{other_server.port}/_testing/image'


@pytest.fixture
def settings(other_server):
    return Settings(
        database_url=os.getenv('DATABASE_URL', DB_DSN),
        redis_database=7,
        master_key=MASTER_KEY,
        grecaptcha_secret='X' * 30,
        grecaptcha_url=f'http://localhost:{other_server.port}/grecaptcha',
        tc_api_root=f'http://localhost:{other_server.port}/api',
        geocoding_url=f'http://localhost:{other_server.port}/geocode',
    )


@pytest.fixture(scope='session')
def db():
    settings_ = Settings(database_url=os.getenv('DATABASE_URL', DB_DSN))
    prepare_database(True, settings_)

    engine = sa_create_engine(settings_.pg_dsn)
    populate_db(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def db_conn(loop, settings, db):
    engine = loop.run_until_complete(aio_create_engine(settings.database_url, loop=loop))
    conn = loop.run_until_complete(engine.acquire())
    transaction = loop.run_until_complete(conn.begin())

    yield conn

    loop.run_until_complete(transaction.rollback())
    loop.run_until_complete(engine.release(conn))
    engine.close()
    loop.run_until_complete(engine.wait_closed())


class MockEngine:
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
def cli(loop, aiohttp_client, db_conn, settings):
    """
    Create an app and client to interact with it

    The postgres pool's acquire method is changed to return a db connection which is in a transaction and is
    used by the test itself.
    """

    async def modify_startup(app):
        app['pg_engine'] = MockEngine(db_conn)
        ctx = {'settings': settings}
        await startup(ctx)
        ctx['pg_engine'] = app['pg_engine']
        redis = app['redis']
        await redis.flushdb()

    app = create_app(loop, settings=settings)
    app.on_startup.append(modify_startup)
    return loop.run_until_complete(aiohttp_client(app))


async def create_company(db_conn, public_key, private_key, name='foobar', domains=['example.com']):
    v = await db_conn.execute(
        sa_companies.insert()
        .values(name=name, public_key=public_key, private_key=private_key, domains=domains)
        .returning(sa_companies.c.id)
    )
    r = await v.first()
    Company = namedtuple('Company', ['public_key', 'private_key', 'id'])
    return Company(public_key, private_key, r.id)


@pytest.fixture
def company(loop, db_conn):
    return loop.run_until_complete(create_company(db_conn, 'thepublickey', 'theprivatekey'))


async def signed_request(cli, url_, *, signing_key_=MASTER_KEY, method_='POST', **data):
    data.setdefault('_request_time', int(time()))
    payload = json.dumps(data)
    b_payload = payload.encode()
    m = hmac.new(signing_key_.encode(), b_payload, hashlib.sha256)
    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    return await cli.request(method_, url_, data=payload, headers=headers)


async def count(db_conn, sa_table):
    cur = await db_conn.execute(select([count_func()]).select_from(sa_table))
    return (await cur.first())[0]


async def select_set(db_conn, *fields, select_from=None):
    q = select(fields)
    if select_from is not None:
        q = q.select_from(select_from)
    return {tuple(cs.values()) async for cs in await db_conn.execute(q)}


async def get(db_conn, model, *where):
    v = await db_conn.execute(select([c for c in model.c]).where(*where))
    v = [r async for r in v]
    if len(v) != 1:
        raise RuntimeError(f'get got wrong number of results: {len(v)} != 1, model: {model}')
    return dict(v[0])


async def create_con_skills(db_conn, *con_ids):
    await db_conn.execute(
        sa_subjects.insert().values(
            [
                {'id': 1, 'name': 'Mathematics', 'category': 'Maths'},
                {'id': 2, 'name': 'Language', 'category': 'English'},
                {'id': 3, 'name': 'Literature', 'category': 'English'},
            ]
        )
    )
    await db_conn.execute(
        sa_qual_levels.insert().values(
            [
                {'id': 11, 'name': 'GCSE', 'ranking': 16},
                {'id': 12, 'name': 'A Level', 'ranking': 18},
                {'id': 13, 'name': 'Degree', 'ranking': 21},
            ]
        )
    )
    skill_ids = [(1, 11), (2, 12)]

    await db_conn.execute(
        sa_con_skills.insert().values(
            [{'contractor': con_id, 'subject': s[0], 'qual_level': s[1]} for con_id, s in product(con_ids, skill_ids)]
        )
    )


async def create_appointment(db_conn, company, create_service=True, service_extra=None, appointment_extra=None):
    service_kwargs = dict(
        id=1,
        company=company.id,
        name='testing service',
        extra_attributes=[
            {'name': 'Foobar', 'type': 'text_short', 'machine_name': 'foobar', 'value': 'this is the value of foobar'}
        ],
        colour='#abc',
    )
    if service_extra:
        service_kwargs.update(service_extra)
    if create_service:
        await db_conn.execute(sa_services.insert().values(**service_kwargs))

    apt_kwargs = dict(
        id=456,
        service=service_kwargs['id'],
        topic='testing appointment',
        attendees_max=42,
        attendees_count=4,
        attendees_current_ids=[1, 2, 3],
        start=datetime(2032, 1, 1, 12, 0, 0),
        finish=datetime(2032, 1, 1, 13, 0, 0),
        price=123.45,
        location='Whatever',
    )
    if appointment_extra:
        apt_kwargs.update(appointment_extra)
    await db_conn.execute(sa_appointments.insert().values(**apt_kwargs))

    return {'appointment': apt_kwargs, 'service': service_kwargs}


@pytest.fixture
def appointment(loop, db_conn, company):
    return loop.run_until_complete(create_appointment(db_conn, company))
