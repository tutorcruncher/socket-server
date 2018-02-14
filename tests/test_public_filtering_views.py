from datetime import datetime
from operator import itemgetter

import pytest
from sqlalchemy import update

from tcsocket.app.models import sa_companies, sa_con_skills, sa_contractors, sa_labels, sa_qual_levels, sa_subjects

from .conftest import create_con_skills, signed_post


async def test_list_contractors_origin(cli, company):
    url = cli.server.app.router['contractor-list'].url_for(company='thepublickey')

    r = await cli.get(url, headers={'Origin': 'http://example.com'})
    assert r.status == 200
    assert r.headers.get('Access-Control-Allow-Origin') == '*'
    assert {'results': [], 'location': None, 'count': 0} == await r.json()

    r = await cli.get(url, headers={'Origin': 'http://different.com'})
    assert r.status == 403
    assert r.headers.get('Access-Control-Allow-Origin') == '*'
    assert {
        'details': "the current Origin 'http://different.com' does not match the allowed domains",
        'status': 'wrong Origin domain'
    } == await r.json()


@pytest.mark.parametrize('domains, origin, response', [
    (['example.com'], 'http://example.com', 200),
    (['example.com'], 'http://www.example.com', 403),
    (['*.example.com'], 'http://www.example.com', 200),
    ([], 'http://example.com', 403),
    (None, 'http://example.com', 200),
    (['localhost'], 'http://localhost:8000', 200),
])
async def test_list_contractors_domains(cli, company, domains, origin, response):
    r = await signed_post(
        cli,
        f'/{company.public_key}/webhook/options',
        signing_key_='this is the master key',
        domains=domains,
    )
    assert r.status == 200, await r.text()

    url = cli.server.app.router['contractor-list'].url_for(company='thepublickey')
    r = await cli.get(url, headers={'Origin': origin})
    assert r.status == response


async def test_list_contractors_referrer(cli, company):
    url = cli.server.app.router['contractor-list'].url_for(company='thepublickey')
    r = await cli.get(url, headers={'Origin': 'https://example.com', 'Referer': 'http://www.whatever.com'})
    assert r.status == 200
    r = await cli.get(url, headers={'Referer': 'http://www.whatever.com'})
    assert r.status == 403
    r = await cli.get(url)
    assert r.status == 200


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
    await create_con_skills(db_conn, 1)

    url = str(cli.server.app.router['contractor-list'].url_for(company=company.public_key))
    r = await cli.get(url + '?' + filter_args)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['count'] == con_count, obj
    assert len(obj['results']) == con_count, obj
    if con_count == 1:
        assert obj['results'][0]['link'] == '1-fred-b'


async def test_filter_contractors_skills_distinct(cli, db_conn, company):
    await db_conn.execute(
        sa_contractors
        .insert()
        .values(id=1, company=company.id, first_name='Fred', last_name='Bloggs', last_updated=datetime.now())
    )
    await create_con_skills(db_conn, 1)
    await db_conn.execute(
        sa_con_skills.insert().values({'contractor': 1, 'subject': 1, 'qual_level': 12})
    )

    url = str(cli.server.app.router['contractor-list'].url_for(company=company.public_key))
    r = await cli.get(url + '?subject=1')
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['count'] == 1, obj
    assert len(obj['results']) == 1, obj


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
    assert obj == {'details': '"subject" had an invalid value "foobar"', 'status': 'invalid_argument'}


async def test_subject_list(cli, db_conn, company):
    await db_conn.execute(
        sa_contractors
        .insert()
        .values([
            dict(id=1, company=company.id, first_name='Fred', last_name='Bloggs', last_updated=datetime.now()),
            dict(id=2, company=company.id, first_name='con2', last_name='tractor', last_updated=datetime.now()),
        ])
    )
    # adding subjects to both cons checks distinct in query
    await create_con_skills(db_conn, 1, 2)

    await db_conn.execute(sa_subjects.insert().values({'id': 4, 'name': 's4', 'category': 'sc4'}))

    r = await cli.get(cli.server.app.router['subject-list'].url_for(company=company.public_key))
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj == [
        {'category': 'English', 'id': 2, 'name': 'Language', 'link': '2-language'},
        {'category': 'Maths', 'id': 1, 'name': 'Mathematics', 'link': '1-mathematics'}
    ]


async def test_qual_level_list(cli, db_conn, company):
    await db_conn.execute(
        sa_contractors
        .insert()
        .values([
            dict(id=1, company=company.id, first_name='Fred', last_name='Bloggs', last_updated=datetime.now()),
            dict(id=2, company=company.id, first_name='con2', last_name='tractor', last_updated=datetime.now()),
        ])
    )
    # adding qual levels to both cons checks distinct in query
    await create_con_skills(db_conn, 1, 2)

    await db_conn.execute(sa_qual_levels.insert().values({'id': 4, 'name': 'ql4', 'ranking': 0}))

    r = await cli.get(cli.server.app.router['qual-level-list'].url_for(company=company.public_key))
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj == [
        {'id': 11, 'name': 'GCSE', 'link': '11-gcse'},
        {'id': 12, 'name': 'A Level', 'link': '12-a-level'}
    ]


@pytest.mark.parametrize('params, con_distances', [
    ({'location': 'SW1W 0EN'}, [('1-bcon-t', 3129), ('2-acon-t', 10054)]),
    ({'location': 'SW1W 0EN', 'max_distance': 4000}, [('1-bcon-t', 3129)]),
    ({'location': 'SW1W 0ENx', 'max_distance': 4000}, [('2-acon-t', None), ('1-bcon-t', None)]),
])
async def test_distance_filter(cli, db_conn, company, params, con_distances):
    await db_conn.execute(
        sa_contractors
        .insert()
        .values([
            dict(id=1, company=company.id, latitude=51.5, longitude=-0.1, first_name='b_con1', last_name='t',
                 last_updated=datetime.now()),
            dict(id=2, company=company.id, latitude=51.5, longitude=0, first_name='a_con2', last_name='t',
                 last_updated=datetime.now()),
        ])
    )

    url = str(cli.server.app.router['contractor-list'].url_for(company=company.public_key))
    r = await cli.get(url, params=params, headers={'X-Forwarded-For': '1.1.1.1', 'CF-IPCountry': 'GB'})
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert list(map(itemgetter('link', 'distance'), obj['results'])) == con_distances


async def test_geocode_cache(cli, other_server, company):
    url = str(cli.server.app.router['contractor-list'].url_for(company=company.public_key))
    country = {'CF-IPCountry': 'GB'}
    r = await cli.get(url, params={'location': 'SW1W 0EN'}, headers={'X-Forwarded-For': '1.1.1.1', **country})
    assert r.status == 200, await r.text()
    assert other_server.app['request_log'] == [('geocode', 'SW1W 0EN|uk')]
    obj = await r.json()
    assert {
        'pretty': 'Lower Grosvenor Pl, Westminster, London SW1W 0EN, UK',
        'lat': 51.4980603,
        'lng': -0.14505
    } == obj['location']

    r = await cli.get(url, params={'location': 'SW1W 0EN'}, headers={'X-Forwarded-For': '1.1.1.2', **country})
    assert r.status == 200, await r.text()
    r = await cli.get(url, params={'location': 'SW1W 0EN'}, headers={'X-Forwarded-For': '1.1.1.3', **country})
    assert r.status == 200, await r.text()
    assert other_server.app['request_log'] == [('geocode', 'SW1W 0EN|uk')]


async def test_geocode_rate_limit(cli, other_server, company):
    url = str(cli.server.app.router['contractor-list'].url_for(company=company.public_key))
    country = {'CF-IPCountry': 'GB'}
    for i in range(20):
        r = await cli.get(url, params={'location': f'SW1W {i}EN'}, headers={'X-Forwarded-For': '1.1.1.1', **country})
        assert r.status == 200, await r.text()
    assert len(other_server.app['request_log']) == 20
    r = await cli.get(url, params={'location': 'SW1W 1ENx'}, headers={'X-Forwarded-For': '1.1.1.1', **country})
    assert r.status == 429, await r.text()
    assert len(other_server.app['request_log']) == 20
    r = await cli.get(url, params={'location': 'SW1W 1ENx'}, headers={'X-Forwarded-For': '1.1.1.1', **country})
    assert r.status == 429, await r.text()
    assert len(other_server.app['request_log']) == 20
    r = await cli.get(url, params={'location': 'SW1W 1ENx'}, headers={'X-Forwarded-For': '1.1.1.2', **country})
    assert r.status == 200, await r.text()
    assert len(other_server.app['request_log']) == 21


async def test_geocode_error(cli, other_server, company):
    url = str(cli.server.app.router['contractor-list'].url_for(company=company.public_key))
    r = await cli.get(url, params={'location': '500'}, headers={'X-Forwarded-For': '1.1.1.1', 'CF-IPCountry': 'GB'})
    assert r.status == 500, await r.text()


async def test_geocode_other_country(cli, other_server, company):
    r = await cli.get(
        cli.server.app.router['contractor-list'].url_for(company=company.public_key),
        params={'location': 'SW1W 0EN'},
        headers={'X-Forwarded-For': '1.1.1.1', 'CF-IPCountry': 'US'}
    )
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert {
        'pretty': 'Lower Grosvenor Pl, Westminster, London SW1W 0EN, UK',
        'lat': 51.4980603,
        'lng': -0.14505,
    } == obj['location']
    assert other_server.app['request_log'] == [('geocode', 'SW1W 0EN|us')]


async def create_labels(db_conn, company):
    await db_conn.execute(
        sa_labels
        .insert()
        .values([
            {'name': 'Apple', 'machine_name': 'apple', 'company': company.id},
            {'name': 'Banana', 'machine_name': 'banana', 'company': company.id},
            {'name': 'Carrot', 'machine_name': 'carrot', 'company': company.id},
        ])
    )


@pytest.mark.parametrize('filter_args, cons', [
    ('', ['1-anne-x', '2-ben-x', '3-charlie-x', '4-dave-x']),
    ('label=apple', ['1-anne-x', '2-ben-x']),
    ('label=apple&label=banana&label=carrot', ['1-anne-x']),
    ('label=banana&label=carrot', ['1-anne-x', '3-charlie-x']),
    ('label_exclude=carrot', ['2-ben-x', '4-dave-x']),
    ('label_exclude=apple&label_exclude=carrot', ['4-dave-x']),
    ('label=apple&label_exclude=carrot', ['2-ben-x']),
])
async def test_label_filter(cli, db_conn, company, filter_args, cons):
    await db_conn.execute(
        sa_contractors
        .insert()
        .values([
            dict(id=1, company=company.id, first_name='Anne', last_name='x', last_updated=datetime.now()),
            dict(id=2, company=company.id, first_name='Ben', last_name='x', last_updated=datetime.now()),
            dict(id=3, company=company.id, first_name='Charlie', last_name='x', last_updated=datetime.now()),
            dict(id=4, company=company.id, first_name='Dave', last_name='x', last_updated=datetime.now()),
        ])
    )
    await create_labels(db_conn, company)

    await db_conn.execute(update(sa_contractors).values(labels=['apple', 'banana', 'carrot'])
                          .where(sa_contractors.c.id == 1))
    await db_conn.execute(update(sa_contractors).values(labels=['apple']).where(sa_contractors.c.id == 2))
    await db_conn.execute(update(sa_contractors).values(labels=['banana', 'carrot']).where(sa_contractors.c.id == 3))

    url = str(cli.server.app.router['contractor-list'].url_for(company=company.public_key))
    r = await cli.get(url + '?sort=name&' + filter_args)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert [c['link'] for c in obj['results']] == cons


async def test_labels_list(cli, db_conn, company):
    url = cli.server.app.router['labels'].url_for(company=company.public_key)

    r = await cli.get(url)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj == {}

    await create_labels(db_conn, company)

    v = await db_conn.execute(
        sa_companies
        .insert()
        .values(name='snap', public_key='snap', private_key='snap', domains=['example.com'])
        .returning(sa_companies.c.id)
    )
    new_company_id = next(r.id for r in v)
    await db_conn.execute(
        sa_labels
        .insert()
        .values({'name': 'Different', 'machine_name': 'different', 'company': new_company_id})
    )

    r = await cli.get(url)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj == {
        'apple': 'Apple',
        'banana': 'Banana',
        'carrot': 'Carrot',
    }


async def test_show_permissions(cli, db_conn, company):
    await db_conn.execute(
        sa_contractors
        .insert()
        .values(id=1, company=company.id, first_name='Fred', last_name='Bloggs', last_updated=datetime.now(),
                labels=['foo', 'bar'], review_rating=3.5, review_duration=1800)
    )

    url = cli.server.app.router['contractor-list'].url_for(company=company.public_key)
    r = await cli.get(url)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['count'] == 1, obj
    results = obj['results']
    assert len(results) == 1, obj
    assert 'labels' not in results[0], results[0]
    assert 'review_rating' not in results[0], results[0]
    assert 'review_duration' not in results[0], results[0]

    await db_conn.execute(update(sa_companies).values(options={
        'show_labels': True, 'show_stars': True, 'show_hours_reviewed': True
    }))

    r = await cli.get(url)
    assert r.status == 200, await r.text()
    obj = await r.json()
    results = obj['results']
    assert results[0]['labels'] == ['foo', 'bar'], results[0]
    assert results[0]['review_rating'] == 3.5, results[0]
    assert results[0]['review_duration'] == 1800, results[0]


@pytest.mark.parametrize('filter_args, con_count, first_id, last_id', [
    ('sort=name', 100, 1, 100),
    ('sort=name&pagination=40', 40, 1, 40),
    ('sort=name&page=1', 100, 1, 100),
    ('sort=name&page=2', 10, 101, 110),
    ('sort=name&pagination=40&page=2', 40, 41, 80),
])
async def test_contractor_pagination(cli, db_conn, company, filter_args, con_count, first_id, last_id):
    cons = [
        dict(id=i, company=company.id, first_name=f'Fred{i:04d}', last_name='X', last_updated=datetime.now())
        for i in range(1, 111)
    ]
    await db_conn.execute(
        sa_contractors
        .insert()
        .values(cons)
    )

    url = str(cli.server.app.router['contractor-list'].url_for(company=company.public_key))
    r = await cli.get(url + '?' + filter_args)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['count'] == 110
    results = obj['results']
    assert len(results) == con_count, obj
    assert results[0]['id'] == first_id, results[0]
    assert results[-1]['id'] == last_id, results[-1]
