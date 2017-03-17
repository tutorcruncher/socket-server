import hashlib
import hmac
import json
from pathlib import Path

from PIL import Image
from sqlalchemy import select
from sqlalchemy.sql.functions import count as count_func

from tcsocket.app.models import sa_con_skills, sa_contractors, sa_qual_levels, sa_subjects

from .conftest import signed_post


async def test_create_master_key(cli, db_conn, company):
    r = await signed_post(
        cli,
        f'/{company.public_key}/contractors/set',
        signing_key_='this is the master key',
        id=123,
        deleted=False,
        first_name='Fred',
        last_name='Bloggs',
    )
    assert r.status == 201, await r.text()
    response_data = await r.json()
    assert response_data == {'details': 'contractor created', 'status': 'success'}
    curr = await db_conn.execute(sa_contractors.select())
    result = await curr.first()
    assert result.id == 123
    assert result.first_name == 'Fred'
    assert result.extra_attributes == []


async def test_create_company_key(cli, db_conn, company):
    r = await signed_post(
        cli,
        f'/{company.public_key}/contractors/set',
        signing_key_=company.private_key,
        id=123,
        deleted=False,
        first_name='Fred',
        last_name='Bloggs',
    )
    assert r.status == 201, await r.text()
    response_data = await r.json()
    assert response_data == {'details': 'contractor created', 'status': 'success'}
    curr = await db_conn.execute(sa_contractors.select())
    result = await curr.first()
    assert result.id == 123
    assert result.first_name == 'Fred'
    assert result.extra_attributes == []


async def test_create_bad_auth(cli, company):
    data = dict(
        id=123,
        deleted=False,
        first_name='Fred',
        last_name='Bloggs',
    )
    payload = json.dumps(data)
    b_payload = payload.encode()
    m = hmac.new(b'this is not the secret key', b_payload, hashlib.sha256)

    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    r = await cli.post(f'/{company.public_key}/contractors/set', data=payload, headers=headers)
    assert r.status == 401


async def test_create_skills(cli, db_conn, company):
    r = await signed_post(
        cli,
        f'/{company.public_key}/contractors/set',
        id=123,
        first_name='Fred',
        skills=[
            {
                'subject_id': 1,
                'qual_level_id': 1,
                'qual_level': 'GCSE',
                'subject': 'Algebra',
                'qual_level_ranking': 16.0,
                'category': 'Maths'
            },
            {
                'subject_id': 2,
                'qual_level_id': 1,
                'qual_level': 'GCSE',
                'subject': 'Language',
                'qual_level_ranking': 16.0,
                'category': 'English'
            }
        ]
    )
    assert r.status == 201, await r.text()
    fields = sa_con_skills.c.contractor, sa_con_skills.c.subject, sa_con_skills.c.qual_level
    con_skills = {tuple(cs.values()) async for cs in await db_conn.execute(select(fields))}
    assert con_skills == {(123, 1, 1), (123, 2, 1)}


async def count(db_conn, sa_table):
    cur = await db_conn.execute(select([count_func()]).select_from(sa_table))
    return (await cur.first())[0]


async def test_modify_skills(cli, db_conn, company):
    r = await signed_post(
        cli,
        f'/{company.public_key}/contractors/set',
        id=123,
        skills=[
            {
                'subject_id': 100,
                'qual_level_id': 200,
                'qual_level': 'GCSE',
                'subject': 'Algebra',
                'category': 'Maths'
            },
            {
                'subject_id': 101,
                'qual_level_id': 200,
                'qual_level': 'GCSE',
                'subject': 'Language',
                'category': 'English'
            }
        ]
    )
    assert r.status == 201, await r.text()
    fields = sa_con_skills.c.contractor, sa_con_skills.c.subject, sa_con_skills.c.qual_level
    con_skills = {tuple(cs.values()) async for cs in await db_conn.execute(select(fields))}
    assert con_skills == {(123, 100, 200), (123, 101, 200)}

    r = await signed_post(
        cli,
        f'/{company.public_key}/contractors/set',
        id=123,
        skills=[
            {
                'subject_id': 102,
                'qual_level_id': 200,
                'qual_level': 'GCSE',
                'subject': 'Literature',
                'category': 'English'
            }
        ]
    )
    assert r.status == 200, await r.text()
    con_skills = {tuple(cs.values()) async for cs in await db_conn.execute(select(fields))}
    assert con_skills == {(123, 102, 200)}

    assert 3 == await count(db_conn, sa_subjects)
    assert 1 == await count(db_conn, sa_qual_levels)


async def test_extra_attributes(cli, db_conn, company):
    eas = [
        {
            'machine_name': None,
            'type': 'checkbox',
            'name': 'Terms and Conditions agreement',
            'value': True,
            'id': 381,
            'sort_index': 0
        },
        {
            'machine_name': 'Bio',
            'type': 'integer',
            'name': 'Teaching Experience',
            'value': 123,
            'id': 196,
            'sort_index': 0.123
        }
    ]
    r = await signed_post(
        cli,
        f'/{company.public_key}/contractors/set',
        id=123,
        deleted=False,
        first_name='Fred',
        extra_attributes=eas
    )
    assert r.status == 201, await r.text()
    curr = await db_conn.execute(sa_contractors.select())
    result = await curr.first()
    assert result.id == 123
    assert result.first_name == 'Fred'
    assert result.extra_attributes == eas
    assert result.tag_line is None
    assert result.primary_description is None


async def test_extra_attributes_special(cli, db_conn, company):
    eas = [
        {
            'machine_name': 'tag_line',
            'type': 'checkbox',
            'name': 'Should be missed',
            'value': True,
            'id': 1,
            'sort_index': 0
        },
        {
            'machine_name': None,
            'type': 'text_short',
            'name': 'Should be missed',
            'value': 'whatever',
            'id': 2,
            'sort_index': 0
        },
        {
            'machine_name': 'tag_line',
            'type': 'text_short',
            'name': 'Should be used',
            'value': 'this is the tag line',
            'id': 3,
            'sort_index': 10
        },
        {
            'machine_name': None,
            'type': 'text_extended',
            'name': 'Primary Description',
            'value': 'Should be used as primary description',
            'id': 4,
            'sort_index': 1
        },
        {
            'machine_name': None,
            'type': 'text_extended',
            'name': 'Not Primary Description',
            'value': 'Should not be used as primary description because it has a higher sort index than above',
            'id': 5,
            'sort_index': 2
        }
    ]
    r = await signed_post(
        cli,
        f'/{company.public_key}/contractors/set',
        id=123,
        deleted=False,
        first_name='Fred',
        extra_attributes=eas
    )
    assert r.status == 201, await r.text()
    curr = await db_conn.execute(sa_contractors.select())
    result = await curr.first()
    assert result.id == 123
    assert result.first_name == 'Fred'
    assert result.tag_line == 'this is the tag line'
    assert result.primary_description == 'Should be used as primary description'
    assert [ea['id'] for ea in result.extra_attributes] == [1, 2, 5]


async def test_photo(cli, db_conn, company, image_download_url, tmpdir):
    r = await signed_post(
        cli,
        f'/{company.public_key}/contractors/set',
        id=123,
        first_name='Fred',
        photo=image_download_url
    )
    assert r.status == 201, await r.text()
    assert [cs.first_name async for cs in await db_conn.execute(sa_contractors.select())] == ['Fred']
    path = Path(tmpdir / 'media' / company.public_key / '123.jpg')
    assert path.exists()
    with Image.open(str(path)) as im:
        assert im.size == (1000, 1000)
    path = Path(tmpdir / 'media' / company.public_key / '123.thumb.jpg')
    assert path.exists()
    with Image.open(str(path)) as im:
        assert im.size == (256, 256)


async def test_update(cli, db_conn, company):
    assert [cs.first_name async for cs in await db_conn.execute(sa_contractors.select())] == []
    r = await signed_post(cli, f'/{company.public_key}/contractors/set', id=123, first_name='Fred')
    assert r.status == 201
    assert [cs.first_name async for cs in await db_conn.execute(sa_contractors.select())] == ['Fred']

    r = await signed_post(cli, f'/{company.public_key}/contractors/set', id=123, first_name='George')
    assert r.status == 200
    assert [cs.first_name async for cs in await db_conn.execute(sa_contractors.select())] == ['George']


async def test_delete(cli, db_conn, company):
    assert 0 == await count(db_conn, sa_contractors)
    r = await signed_post(cli, f'/{company.public_key}/contractors/set', id=123, first_name='Fred')
    assert r.status == 201
    assert 1 == await count(db_conn, sa_contractors)

    r = await signed_post(cli, f'/{company.public_key}/contractors/set', id=123, deleted=True)
    assert r.status == 200
    assert 0 == await count(db_conn, sa_contractors)

    r = await signed_post(cli, f'/{company.public_key}/contractors/set', id=123, deleted=True)
    assert r.status == 404
    assert 0 == await count(db_conn, sa_contractors)


async def test_delete_all_fields(cli, db_conn, company):
    assert 0 == await count(db_conn, sa_contractors)
    r = await signed_post(cli, f'/{company.public_key}/contractors/set', id=123, first_name='Fred')
    assert r.status == 201
    assert 1 == await count(db_conn, sa_contractors)

    data = {
        'country': None,
        'created': None,
        'deleted': True,
        'extra_attributes': [],
        'first_name': None,
        'id': 123,
        'labels': [],
        'last_name': None,
        'last_updated': None,
        'location': None,
        'photo': None,
        'release_timestamp': '2032-02-06T14:17:05.548260Z',
        'skills': [],
        'town': None
    }

    r = await signed_post(cli, f'/{company.public_key}/contractors/set', **data)
    assert r.status == 200, await r.text()
    assert 0 == await count(db_conn, sa_contractors)

    r = await signed_post(cli, f'/{company.public_key}/contractors/set', id=123, deleted=True)
    assert r.status == 404
    assert 0 == await count(db_conn, sa_contractors)


async def test_delete_skills(cli, db_conn, company):
    r = await signed_post(
        cli,
        f'/{company.public_key}/contractors/set',
        id=123,
        skills=[
            {
                'subject_id': 1,
                'qual_level_id': 1,
                'qual_level': 'GCSE',
                'subject': 'Literature',
                'category': 'English'
            }
        ]
    )
    assert r.status == 201, await r.text()
    assert 1 == await count(db_conn, sa_contractors)
    assert 1 == await count(db_conn, sa_con_skills)
    assert 1 == await count(db_conn, sa_subjects)
    assert 1 == await count(db_conn, sa_qual_levels)

    r = await signed_post(cli, f'/{company.public_key}/contractors/set', id=123, deleted=True)
    assert r.status == 200
    assert 0 == await count(db_conn, sa_contractors)
    assert 0 == await count(db_conn, sa_con_skills)
    assert 1 == await count(db_conn, sa_subjects)
    assert 1 == await count(db_conn, sa_qual_levels)


async def test_invalid_json(cli, company):
    payload = 'foobar'
    b_payload = payload.encode()
    m = hmac.new(b'this is the master key', b_payload, hashlib.sha256)

    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    r = await cli.post(f'/{company.public_key}/contractors/set', data=payload, headers=headers)
    assert r.status == 400, await r.text()
    response_data = await r.json()
    assert response_data == {
        'details': 'Value Error: Expecting value: line 1 column 1 (char 0)',
        'status': 'invalid request data',
    }


async def test_invalid_schema(cli, company):
    r = await signed_post(
        cli,
        f'/{company.public_key}/contractors/set',
        id='not an int',
    )
    assert r.status == 400, await r.text()
    response_data = await r.json()
    assert response_data == {
        'details': {'id': "value can't be converted to int"},
        'status': 'invalid request data',
    }


async def test_missing_company(cli, company):
    r = await signed_post(
        cli,
        f'/not-{company.public_key}/contractors/set',
        id=123,
    )
    assert r.status == 404, await r.text()
    response_data = await r.json()
    assert response_data == {
        'details': 'No company found for key not-thepublickey',
        'status': 'company not found',
    }
