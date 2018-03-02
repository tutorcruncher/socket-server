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
    ] == obj['results']


async def test_list_contractors_name(cli, db_conn, company):
    await db_conn.execute(
        sa_contractors
        .insert()
        .values(id=1, company=company.id, first_name='Fred', last_name='Bloggs', last_updated=datetime.now())
    )
    r = await cli.get(cli.server.app.router['contractor-list'].url_for(company='thepublickey'))
    assert r.status == 200, await r.text()
    assert (await r.json())['results'][0]['link'] == '1-fred-b'
    assert (await r.json())['results'][0]['name'] == 'Fred B'

    await db_conn.execute((
        update(sa_companies)
        .values({'name_display': NameOptions.first_name})
        .where(sa_companies.c.public_key == company.public_key)
    ))
    r = await cli.get(cli.server.app.router['contractor-list'].url_for(company='thepublickey'))
    assert r.status == 200, await r.text()
    assert (await r.json())['results'][0]['link'] == '1-fred'
    assert (await r.json())['results'][0]['name'] == 'Fred'

    await db_conn.execute((
        update(sa_companies)
        .values({'name_display': NameOptions.full_name})
        .where(sa_companies.c.public_key == company.public_key)
    ))
    r = await cli.get(cli.server.app.router['contractor-list'].url_for(company='thepublickey'))
    assert r.status == 200, await r.text()
    assert (await r.json())['results'][0]['link'] == '1-fred-bloggs'
    assert (await r.json())['results'][0]['name'] == 'Fred Bloggs'


@pytest.mark.parametrize('headers, newline_count', [
    ({'Accept': 'application/json'}, 0),
    ({'Accept': '*/*'}, 18),
    (None, 18),
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

    r = await cli.get(cli.server.app.router['contractor-get'].url_for(company='thepublickey', id=str(con_id), slug='x'))
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
    url = cli.server.app.router['contractor-get'].url_for(company='thepublickey', id='1', slug='x')
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
    url = cli.server.app.router['contractor-get'].url_for(company='thepublickey', id='1', slug='x')
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
