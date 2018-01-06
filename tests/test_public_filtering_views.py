from datetime import datetime
from operator import itemgetter

import pytest

from tcsocket.app.models import sa_con_skills, sa_contractors, sa_qual_levels, sa_subjects

from .conftest import create_con_skills_labels


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
    await create_con_skills_labels(db_conn, company.id, 1)

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
    await create_con_skills_labels(db_conn, company.id, 1)
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
    await create_con_skills_labels(db_conn, company.id, 1, 2)

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
    await create_con_skills_labels(db_conn, company.id, 1, 2)

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


async def test_label_filter(cli, db_conn, company):
    await db_conn.execute(
        sa_contractors
        .insert()
        .values([
            dict(id=1, company=company.id, first_name='Fred', last_name='Bloggs', last_updated=datetime.now()),
            dict(id=2, company=company.id, first_name='con2', last_name='tractor', last_updated=datetime.now()),
        ])
    )
    await create_con_skills_labels(db_conn, company.id, 1)

    url = str(cli.server.app.router['contractor-list'].url_for(company=company.public_key))
    r = await cli.get(url + '?label=carrot')
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert len(obj) == 1
    assert obj[0]['link'] == '1-fred-b'

    url = str(cli.server.app.router['contractor-list'].url_for(company=company.public_key))
    r = await cli.get(url + '?label_exclude=carrot')
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert len(obj) == 1
    assert obj[0]['link'] == '2-con-t', obj
