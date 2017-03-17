import json
from datetime import datetime

import pytest

from tcsocket.app.models import sa_companies, sa_con_skills, sa_contractors, sa_qual_levels, sa_subjects


async def test_index(cli):
    r = await cli.get('/')
    assert r.status == 200
    assert "You're looking at TutorCruncher socket's API" in await r.text()


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
    assert r.status == 200
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
            'url': 'https://socket.tutorcruncher.com/thepublickey/contractors/1',
        }
    ] == obj


async def create_skills(db_conn, con_id):
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
    ids = [(1, 11), (2, 12)]

    await db_conn.execute(
        sa_con_skills
        .insert()
        .values([{'contractor': con_id, 'subject': s, 'qual_level': ql} for s, ql in ids])
    )


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
            extra_attributes=[{'foo': 'bar'}]
        )
        .returning(sa_contractors.c.id)
    )
    con_id = (await v.first()).id
    await create_skills(db_conn, con_id)

    r = await cli.get(cli.server.app.router['contractor-get'].url_for(company='thepublickey', id=con_id, slug='x'))
    assert r.status == 200
    obj = await r.json()
    assert {
        'id': 1,
        'name': 'Fred B',
        'town': None,
        'country': None,
        'extra_attributes': [{'foo': 'bar'}],
        'tag_line': None,
        'photo': 'https://socket.tutorcruncher.com/media/thepublickey/1.jpg',
        'primary_description': None,
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


async def test_get_enquiry(cli, company, other_server):
    r = await cli.get(cli.server.app.router['enquiry'].url_for(company=company.public_key))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert len(data) == 3
    assert data['visible'][0]['field'] == 'client_name'
    assert data['visible'][0]['max_length'] == 255
    assert data['last_updated'] == 0
    # once to get immediate response, once "on the worker"
    assert other_server.app['request_log'] == ['enquiry_options', 'enquiry_options']

    r = await cli.get(cli.server.app.router['enquiry'].url_for(company=company.public_key))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert len(data) == 3
    assert 1e9 < data['last_updated'] < 2e9
    # no more requests as data came from cache
    assert other_server.app['request_log'] == ['enquiry_options', 'enquiry_options']


async def test_post_enquiry(cli, company, other_server):
    data = {
        'client_name': 'Cat Flap',
        'client_phone': '123',
        'grecaptcha_response': 'good' * 5,
    }
    url = cli.server.app.router['enquiry'].url_for(company=company.public_key)
    r = await cli.post(url, data=json.dumps(data), headers={'User-Agent': 'Testing Browser'})
    assert r.status == 201, await r.text()
    data = await r.json()
    assert data == {'status': 'enquiry submitted to TutorCruncher'}
    assert other_server.app['request_log'] == [
        ('grecaptcha_post', {
            'secret': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
            'response': 'goodgoodgoodgoodgood'
        }),
        ('enquiry_post', {
            'client_name': 'Cat Flap',
            'client_phone': '123',
            'user_agent': 'Testing Browser',
            'ip_address': None,
            'http_referrer': None}
         )
    ]


async def test_post_enquiry_bad_captcha(cli, company, other_server):
    data = {
        'client_name': 'Cat Flap',
        'client_phone': '123',
        'grecaptcha_response': 'bad_' * 5,
    }
    url = cli.server.app.router['enquiry'].url_for(company=company.public_key)
    r = await cli.post(url, data=json.dumps(data), headers={'X-Forward-For': '1.2.3.4'})
    assert r.status == 201, await r.text()
    assert other_server.app['request_log'] == [
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
        ('grecaptcha_post', {
            'secret': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
            'response': 'goodgoodgoodgoodgood'
        })
    ]


async def test_list_contractors_origin(cli, company):
    url = cli.server.app.router['contractor-list'].url_for(company='thepublickey')
    r = await cli.get(url, headers={'Origin': 'http://www.example.com'})
    assert r.status == 200
    assert r.headers.get('Access-Control-Allow-Origin') == 'http://www.example.com'
    assert [] == await r.json()

    url = cli.server.app.router['contractor-list'].url_for(company='thepublickey')
    r = await cli.get(url, headers={'Origin': 'http://example.com'})
    assert r.status == 200
    assert r.headers.get('Access-Control-Allow-Origin') == 'http://example.com'
    assert [] == await r.json()

    url = cli.server.app.router['contractor-list'].url_for(company='thepublickey')
    r = await cli.get(url, headers={'Origin': 'http://different.com'})
    assert r.status == 200
    assert r.headers.get('Access-Control-Allow-Origin') == 'http://example.com'
    assert [] == await r.json()


@pytest.mark.parametrize('filter_args, con_count', [
    ('', 2),
    ('subject=1', 1),
    ('subject=2', 1),
    ('subject=3', 0),
    ('qual_level=11', 1),
    ('qual_level=12', 1),
    ('qual_level=13', 0),
    ('subject=1&qual_level=11', 1),
    ('subject=3&qual_level=11', 0),
])
async def test_filter_contractors_skills(cli, db_conn, company, filter_args, con_count):
    await db_conn.execute(
        sa_contractors
        .insert()
        .values([
            dict(id=1, company=company.id, first_name='Fred', last_name='Bloggs', last_updated=datetime.now()),
            dict(id=2, company=company.id, first_name='con2', last_name='tractor', last_updated=datetime.now()),
        ])
    )
    await create_skills(db_conn, 1)

    url = str(cli.server.app.router['contractor-list'].url_for(company=company.public_key))
    r = await cli.get(url + '?' + filter_args)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert len(obj) == con_count
    if con_count == 1:
        assert obj[0]['link'] == '1-fred-b'


async def test_filter_contractors_skills_invalid(cli, db_conn, company):
    await db_conn.execute(
        sa_contractors
        .insert()
        .values(id=1, company=company.id, first_name='Fred', last_name='Bloggs', last_updated=datetime.now())
    )

    url = str(cli.server.app.router['contractor-list'].url_for(company=company.public_key)) + '?subject=foobar'
    r = await cli.get(url)
    assert r.status == 400, await r.text()
    obj = await r.json()
    assert obj == {'details': '"subject" had an invalid value "foobar"', 'status': 'invalid_filter'}
