import json
from datetime import datetime

import pytest
from aiohttp.web import Application
from sqlalchemy import update

from tcsocket.app import middleware
from tcsocket.app.models import NameOptions, sa_companies, sa_contractors

from .conftest import create_con_skills


async def test_index(cli):
    r = await cli.get('/')
    assert r.status == 200
    assert "You're looking at TutorCruncher socket's API" in await r.text()


async def test_index_head(cli):
    r = await cli.head('/')
    assert r.status == 200
    assert '' == await r.text()


async def test_robots(cli):
    r = await cli.get('/robots.txt')
    assert r.status == 200
    assert 'User-agent: *' in await r.text()


async def test_favicon(cli, mocker):
    mocker.spy(middleware, 'log_warning')
    r = await cli.get('/favicon.ico', allow_redirects=False)
    assert r.status == 301
    assert r.headers['Location'] == 'https://secure.tutorcruncher.com/favicon.ico'
    assert middleware.log_warning.call_count == 0


async def test_list_contractors(cli, db_conn):
    v = await db_conn.execute(
        sa_companies
        .insert()
        .values(name='testing', public_key='thepublickey', private_key='theprivatekey')
        .returning(sa_companies.c.id)
    )
    r = await v.first()
    company_id = r.id
    await db_conn.execute(
        sa_contractors
        .insert()
        .values(id=1, company=company_id, first_name='Fred', last_name='Bloggs', last_updated=datetime.now())
    )
    headers = {
        'HOST': 'www.example.com',
    }
    r = await cli.get(cli.server.app.router['contractor-list'].url_for(company='thepublickey'), headers=headers)
    assert r.status == 200, await r.text()
    assert r.headers.get('Access-Control-Allow-Origin') == '*'
    obj = await r.json()
    assert [
        {
            'id': 1,
            'link': '1-fred-b',
            'name': 'Fred B',
            'photo': 'https://socket.tutorcruncher.com/media/thepublickey/1.thumb.jpg',
            'tag_line': None,
            'primary_description': None,
            'town': None,
            'country': None,
            'distance': None,
            'url': 'https://socket.tutorcruncher.com/thepublickey/contractors/1',
        }
    ] == obj


async def test_list_contractors_name(cli, db_conn, company):
    await db_conn.execute(
        sa_contractors
        .insert()
        .values(id=1, company=company.id, first_name='Fred', last_name='Bloggs', last_updated=datetime.now())
    )
    r = await cli.get(cli.server.app.router['contractor-list'].url_for(company='thepublickey'))
    assert r.status == 200, await r.text()
    assert (await r.json())[0]['link'] == '1-fred-b'
    assert (await r.json())[0]['name'] == 'Fred B'

    await db_conn.execute((
        update(sa_companies)
        .values({'name_display': NameOptions.first_name})
        .where(sa_companies.c.public_key == company.public_key)
    ))
    r = await cli.get(cli.server.app.router['contractor-list'].url_for(company='thepublickey'))
    assert r.status == 200, await r.text()
    assert (await r.json())[0]['link'] == '1-fred'
    assert (await r.json())[0]['name'] == 'Fred'

    await db_conn.execute((
        update(sa_companies)
        .values({'name_display': NameOptions.full_name})
        .where(sa_companies.c.public_key == company.public_key)
    ))
    r = await cli.get(cli.server.app.router['contractor-list'].url_for(company='thepublickey'))
    assert r.status == 200, await r.text()
    assert (await r.json())[0]['link'] == '1-fred-bloggs'
    assert (await r.json())[0]['name'] == 'Fred Bloggs'


@pytest.mark.parametrize('headers, newline_count', [
    ({'Accept': 'application/json'}, 0),
    ({'Accept': '*/*'}, 14),
    (None, 14),
])
async def test_json_encoding(cli, db_conn, company, headers, newline_count):
    await db_conn.execute(
        sa_contractors
        .insert()
        .values(id=1, company=company.id, first_name='Fred', last_name='Bloggs', last_updated=datetime.now())
    )
    r = await cli.get(cli.server.app.router['contractor-list'].url_for(company='thepublickey'), headers=headers)
    assert r.status == 200
    assert (await r.text()).count('\n') == newline_count


async def test_get_contractor(cli, db_conn):
    v = await db_conn.execute(
        sa_companies
        .insert()
        .values(name='testing', public_key='thepublickey', private_key='theprivatekey')
        .returning(sa_companies.c.id)
    )
    r = await v.first()
    company_id = r.id
    v = await db_conn.execute(
        sa_contractors
        .insert()
        .values(
            id=1,
            company=company_id,
            first_name='Fred',
            last_name='Bloggs',
            last_updated=datetime.now(),
            extra_attributes=[
                {'sort_index': 5, 'foo': 'bar'},
                {'foo': 'apple'},
                {'sort_index': 1, 'foo': 'spam'},
            ]
        )
        .returning(sa_contractors.c.id)
    )
    con_id = (await v.first()).id
    await create_con_skills(db_conn, con_id)

    r = await cli.get(cli.server.app.router['contractor-get'].url_for(company='thepublickey', id=con_id, slug='x'))
    assert r.status == 200
    obj = await r.json()
    assert {
        'id': 1,
        'name': 'Fred B',
        'town': None,
        'country': None,
        'extra_attributes': [
            {'sort_index': 1, 'foo': 'spam'},
            {'sort_index': 5, 'foo': 'bar'},
            {'foo': 'apple'},
        ],
        'labels': [],
        'tag_line': None,
        'photo': 'https://socket.tutorcruncher.com/media/thepublickey/1.jpg',
        'primary_description': None,
        'review_duration': None,
        'review_rating': None,
        'skills': [
            {
                'category': 'English',
                'qual_levels': ['A Level'],
                'subject': 'Language'
            },
            {
                'category': 'Maths',
                'qual_levels': ['GCSE'],
                'subject': 'Mathematics'
            }
        ],
    } == obj


async def test_missing_url(cli):
    r = await cli.get('/foobar')
    assert r.status == 404, await r.text()


async def test_url_trailing_slash(cli, company):
    url = cli.server.app.router['contractor-list'].url_for(company='thepublickey')
    r = await cli.get(url)
    assert r.status == 200, await r.text()
    r = await cli.get(f'{url}/', allow_redirects=False)
    assert r.status == 301, await r.text()
    assert r.headers['location'] == str(url)


async def test_get_enquiry(cli, company, other_server):
    other_server.app['extra_attributes'] = 'default'
    r = await cli.get(cli.server.app.router['enquiry'].url_for(company=company.public_key))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert len(data) == 2
    assert len(data['visible']) == 7
    assert data['visible'][0]['field'] == 'client_name'
    assert data['visible'][0]['max_length'] == 255
    date_field = next(f for f in data['visible'] if f['field'] == 'date-of-birth')
    assert date_field['label'] == 'Date of Birth'
    assert date_field['prefix'] == 'attributes'
    assert date_field['type'] == 'date'
    # once to get immediate response, once "on the worker"
    assert other_server.app['request_log'] == ['enquiry_options']

    r = await cli.get(cli.server.app.router['enquiry'].url_for(company=company.public_key))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert len(data) == 2
    assert len(data['visible']) == 7
    # no more requests as data came from cache
    assert other_server.app['request_log'] == ['enquiry_options']


async def test_get_enquiry_repeat(cli, company, other_server):
    other_server.app['extra_attributes'] = 'default'
    r = await cli.get(cli.server.app.router['enquiry'].url_for(company=company.public_key))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert len(data['visible']) == 7
    assert other_server.app['request_log'] == ['enquiry_options']

    r = await cli.get(cli.server.app.router['enquiry'].url_for(company=company.public_key))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert len(data['visible']) == 7
    assert other_server.app['request_log'] == ['enquiry_options']

    redis = await cli.server.app['worker'].get_redis()
    raw_enquiry_options = await redis.get(b'enquiry-data-%d' % company.id)
    enquiry_options = json.loads(raw_enquiry_options.decode())
    enquiry_options['last_updated'] -= 2000
    await redis.set(b'enquiry-data-%d' % company.id, json.dumps(enquiry_options).encode())

    r = await cli.get(cli.server.app.router['enquiry'].url_for(company=company.public_key))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert len(data['visible']) == 7
    assert other_server.app['request_log'] == ['enquiry_options', 'enquiry_options']


async def test_post_enquiry_success(cli, company, other_server):
    other_server.app['extra_attributes'] = 'default'
    data = {
        'client_name': 'Cat Flap',
        'client_phone': '123',
        'grecaptcha_response': 'good' * 5,
        'attributes': {
            'tell-us-about-yourself': 'hello',
            'how-did-you-hear-about-us': 'foo',
            'date-of-birth': 1969660800
        }
    }
    url = cli.server.app.router['enquiry'].url_for(company=company.public_key)
    r = await cli.post(url, data=json.dumps(data), headers={'User-Agent': 'Testing Browser'})
    assert r.status == 201, await r.text()
    data = await r.json()
    assert data == {'status': 'enquiry submitted to TutorCruncher'}
    assert [
        'enquiry_options',
        (
            'grecaptcha_post',
            {
                'secret': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
                'response': 'goodgoodgoodgoodgood',
            },
        ),
        (
            'enquiry_post',
            {
                'client_name': 'Cat Flap',
                'client_phone': '123',
                'user_agent': 'Testing Browser',
                'ip_address': None,
                'http_referrer': None,
                'attributes': {
                    'tell-us-about-yourself': 'hello',
                    'how-did-you-hear-about-us': 'foo',
                    'date-of-birth': '2032-06-01',
                },
            },
        ),
    ] == other_server.app['request_log']


async def test_post_enquiry_datetime(cli, company, other_server):
    other_server.app['extra_attributes'] = 'datetime'
    data = {
        'client_name': 'Cat Flap',
        'grecaptcha_response': 'good' * 5,
        'attributes': {
            'date-field': '2032-06-01',
            'datetime-field': '2018-02-07T14:45',
        }
    }
    url = cli.server.app.router['enquiry'].url_for(company=company.public_key)
    r = await cli.post(url, data=json.dumps(data), headers={'User-Agent': 'Testing Browser'})
    assert r.status == 201, await r.text()
    assert [
        'enquiry_options',
        (
            'grecaptcha_post',
            {
                'secret': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
                'response': 'goodgoodgoodgoodgood',
            },
        ),
        (
            'enquiry_post',
            {
                'client_name': 'Cat Flap',
                'user_agent': 'Testing Browser',
                'ip_address': None,
                'http_referrer': None,
                'attributes': {
                    'date-field': '2032-06-01',
                    'datetime-field': '2018-02-07T14:45:00',
                },
            },

        ),
    ] == other_server.app['request_log']


async def test_post_enquiry_invalid_attributes(cli, company, other_server):
    other_server.app['extra_attributes'] = 'default'
    data = {
        'client_name': 'Cat Flap',
        'client_phone': '123',
        'grecaptcha_response': 'good' * 5,
        'attributes': {
            'how-did-you-hear-about-us': 'spam',
            'date-of-birth': 'xxx'
        }
    }
    url = cli.server.app.router['enquiry'].url_for(company=company.public_key)
    r = await cli.post(url, data=json.dumps(data), headers={'User-Agent': 'Testing Browser'})
    assert r.status == 400, await r.text()
    data = await r.json()
    assert data == {
        'details': {
            'date-of-birth': {
                'error_msg': 'Invalid date format',
                'error_type': 'ValueError',
                'track': 'date',
            },
            'how-did-you-hear-about-us': {
                'error_msg': "'spam' is not a valid DynamicEnum",
                'error_type': 'ValueError',
                'track': 'DynamicEnum',
            },
            'tell-us-about-yourself': {
                'error_msg': 'field required',
                'error_type': 'Missing',
            },
        },
        'status': 'invalid attribute data',
    }


async def test_post_enquiry_bad_captcha(cli, company, other_server):
    data = {
        'client_name': 'Cat Flap',
        'client_phone': '123',
        'grecaptcha_response': 'bad_' * 5,
    }
    url = cli.server.app.router['enquiry'].url_for(company=company.public_key)
    r = await cli.post(url, data=json.dumps(data), headers={'X-Forwarded-For': '1.2.3.4'})
    assert r.status == 201, await r.text()
    assert other_server.app['request_log'] == [
        'enquiry_options',
        ('grecaptcha_post', {
            'secret': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
            'response': 'bad_bad_bad_bad_bad_',
            'remoteip': '1.2.3.4'
        }),
    ]


async def test_post_enquiry_wrong_captcha_domain(cli, company, other_server):
    data = {
        'client_name': 'Cat Flap',
        'client_phone': '123',
        'grecaptcha_response': 'good' * 5,
    }
    other_server.app['grecaptcha_host'] = 'other.com'
    url = cli.server.app.router['enquiry'].url_for(company=company.public_key)
    r = await cli.post(url, data=json.dumps(data), headers={'User-Agent': 'Testing Browser'})
    assert r.status == 201, await r.text()
    assert other_server.app['request_log'] == [
        'enquiry_options',
        ('grecaptcha_post', {
            'secret': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
            'response': 'goodgoodgoodgoodgood'
        })
    ]


async def test_post_enquiry_400(cli, company, other_server, caplog):
    other_server.app['extra_attributes'] = 'default'
    data = {
        'client_name': 'Cat Flap',
        'client_phone': '123',
        'grecaptcha_response': 'good' * 5,
        'attributes': {'tell-us-about-yourself': 'hello'},
    }
    headers = {
        'User-Agent': 'Testing Browser',
        'Origin': 'http://example.com',
        'Referer': 'http://cause400.com',
    }
    url = cli.server.app.router['enquiry'].url_for(company=company.public_key)
    r = await cli.post(url, data=json.dumps(data), headers=headers)
    assert r.status == 201, await r.text()
    data = await r.json()
    assert data == {'status': 'enquiry submitted to TutorCruncher'}
    assert other_server.app['request_log'] == [
        'enquiry_options',
        (
            'grecaptcha_post',
            {
                'secret': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
                'response': 'goodgoodgoodgoodgood',
            },
        ),
        (
            'enquiry_post',
            {
                'client_name': 'Cat Flap',
                'client_phone': '123',
                'user_agent': 'Testing Browser',
                'ip_address': None,
                'http_referrer': 'http://cause400.com',
                'attributes': {'tell-us-about-yourself': 'hello'},

            },
        ),
        'enquiry_options',
    ]
    assert '400 response forwarding enquiry to http://localhost:' in caplog


async def test_post_enquiry_skip_grecaptcha(cli, company, other_server):
    data = {
        'client_name': 'Cat Flap',
        'upstream_http_referrer': 'foobar',
        'grecaptcha_response': 'mock-grecaptcha:{.private_key}'.format(company),
    }
    url = cli.server.app.router['enquiry'].url_for(company=company.public_key)
    r = await cli.post(url, data=json.dumps(data), headers={'User-Agent': 'Testing Browser'})
    assert r.status == 201, await r.text()
    data = await r.json()
    assert data == {'status': 'enquiry submitted to TutorCruncher'}
    assert other_server.app['request_log'] == [
        'enquiry_options',
        (
            'enquiry_post',
            {
                'client_name': 'Cat Flap',
                'upstream_http_referrer': 'foobar',
                'user_agent': 'Testing Browser',
                'ip_address': None,
                'http_referrer': None,
            },
        ),
    ]


async def test_post_enquiry_500(cli, company, other_server, caplog):
    data = {
        'client_name': 'Cat Flap',
        'grecaptcha_response': 'good' * 5,
        'attributes': {'tell-us-about-yourself': 'hello'},
    }
    headers = {'Referer': 'http://snap.com', 'Origin': 'http://example.com'}
    url = cli.server.app.router['enquiry'].url_for(company=company.public_key)
    r = await cli.post(url, data=json.dumps(data), headers=headers)
    # because jobs are being executed directly
    assert r.status == 500, await r.text()
    assert 'Bad response from http://localhost:' in caplog


async def test_post_enquiry_referrer_too_long(cli, company, other_server):
    data = {
        'client_name': 'Cat Flap',
        'client_phone': '123',
        'grecaptcha_response': 'good' * 5,
        'upstream_http_referrer': 'X' * 2000,
        'attributes': {'tell-us-about-yourself': 'hello'},
    }
    url = cli.server.app.router['enquiry'].url_for(company=company.public_key)
    headers = {'User-Agent': 'Testing Browser', 'Referer': 'Y' * 2000, 'Origin': 'http://example.com'}
    r = await cli.post(url, data=json.dumps(data), headers=headers)
    assert r.status == 201, await r.text()
    data = await r.json()
    assert data == {'status': 'enquiry submitted to TutorCruncher'}
    assert other_server.app['request_log'][2][1]['upstream_http_referrer'] == 'X' * 1023
    assert other_server.app['request_log'][2][1]['http_referrer'] == 'Y' * 1023


async def snap(request):
    raise RuntimeError('snap')


async def test_500_error(test_client, caplog):
    app = Application(middlewares=[middleware.error_middleware])
    app.router.add_get('/', snap)
    client = await test_client(app)
    r = await client.get('/')
    assert r.status == 500
    assert '500: Internal Server Error' == await r.text()
    assert 'socket.request ERROR: RuntimeError: snap' in caplog


async def test_view_labels(cli, db_conn, company):
    await db_conn.execute(
        sa_contractors
        .insert()
        .values(dict(id=1, company=company.id, first_name='Anne', last_name='x', last_updated=datetime.now()))
    )
    url = cli.server.app.router['contractor-get'].url_for(company='thepublickey', id=1, slug='x')
    r = await cli.get(url)
    assert r.status == 200
    assert (await r.json())['labels'] == []

    await db_conn.execute(update(sa_contractors).values(labels=['apple', 'banana', 'carrot'])
                          .where(sa_contractors.c.id == 1))

    r = await cli.get(url)
    assert r.status == 200
    assert (await r.json())['labels'] == []

    await db_conn.execute(update(sa_companies).values(options={'show_labels': True}))

    r = await cli.get(url)
    assert r.status == 200
    assert (await r.json())['labels'] == ['apple', 'banana', 'carrot']


async def test_review_display(cli, db_conn, company):
    await db_conn.execute(
        sa_contractors
        .insert()
        .values(dict(id=1, company=company.id, first_name='Anne', last_name='x', last_updated=datetime.now(),
                     review_rating=4.249, review_duration=7200))
    )
    url = cli.server.app.router['contractor-get'].url_for(company='thepublickey', id=1, slug='x')
    r = await cli.get(url)
    assert r.status == 200
    obj = await r.json()
    assert (obj['review_rating'], obj['review_duration']) == (None, None)

    await db_conn.execute(update(sa_companies).values(options={'show_stars': True}))

    r = await cli.get(url)
    obj = await r.json()
    assert (obj['review_rating'], obj['review_duration']) == (4.249, None)

    await db_conn.execute(update(sa_companies).values(options={'show_stars': True, 'show_hours_reviewed': True}))

    r = await cli.get(url)
    obj = await r.json()
    assert (obj['review_rating'], obj['review_duration']) == (4.249, 7200)
