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
    assert [] == await r.json()

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
    assert len(obj) == con_count
    if con_count == 1:
        assert obj[0]['link'] == '1-fred-b'


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
    assert len(obj) == 1, obj


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


async def test_distance_filter(cli, db_conn, company):
    await db_conn.execute(
        sa_contractors
        .insert()
        .values([
            dict(id=1, company=company.id, latitude=50, longitude=0, first_name='b_con1', last_name='t',
                 last_updated=datetime.now()),
            dict(id=2, company=company.id, latitude=50, longitude=-0.1, first_name='a_con2', last_name='t',
                 last_updated=datetime.now()),
        ])
    )

    base_url = str(cli.server.app.router['contractor-list'].url_for(company=company.public_key))
    r = await cli.get(base_url + '?latitude=50.1&longitude=0&sort=distance')
    assert r.status == 200, await r.text()
    obj = await r.json()
    link_distance = list(map(itemgetter('link', 'distance'), obj))
    assert link_distance == [('1-bcon-t', 11132), ('2-acon-t', 13229)]

    r = await cli.get(base_url + '?latitude=50.1&longitude=0&sort=name')
    assert r.status == 200, await r.text()
    obj = await r.json()
    link_distance = list(map(itemgetter('link', 'distance'), obj))
    assert link_distance == [('2-acon-t', 13229), ('1-bcon-t', 11132)]

    r = await cli.get(base_url + '?latitude=50.1&sort=distance')
    assert r.status == 400, await r.text()
    assert (await r.json()) == {
        'details': 'distance sorting not available if latitude and longitude are not provided',
        'status': 'invalid_argument'
    }


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
    r = await cli.get(url + '?' + filter_args)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert [c['link'] for c in obj] == cons


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
