import hashlib
import hmac
import json
from pathlib import Path
from time import time

import pytest
from PIL import Image

from tcsocket.app.models import sa_con_skills, sa_contractors, sa_labels, sa_qual_levels, sa_subjects

from .conftest import count, get, select_set, signed_request


async def test_create_master_key(cli, db_conn, company):
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
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
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
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
        _request_time=time()
    )
    payload = json.dumps(data)
    b_payload = payload.encode()
    m = hmac.new(b'this is not the secret key', b_payload, hashlib.sha256)

    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    r = await cli.post(f'/{company.public_key}/webhook/contractor', data=payload, headers=headers)
    assert r.status == 401, await r.text()


async def test_create_skills(cli, db_conn, company):
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
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
    con_skills = await select_set(db_conn,
                                  sa_con_skills.c.contractor, sa_con_skills.c.subject, sa_con_skills.c.qual_level)
    assert con_skills == {(123, 1, 1), (123, 2, 1)}


async def test_modify_skills(cli, db_conn, company):
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
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
    con_skills = await select_set(db_conn, *fields)
    assert con_skills == {(123, 100, 200), (123, 101, 200)}

    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
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
    con_skills = await select_set(db_conn, *fields)
    assert con_skills == {(123, 102, 200)}

    assert 3 == await count(db_conn, sa_subjects)
    assert 1 == await count(db_conn, sa_qual_levels)


async def test_extra_attributes(cli, db_conn, company):
    eas = [
        {
            'machine_name': 'terms',
            'type': 'checkbox',
            'name': 'Terms and Conditions agreement',
            'value': True,
            'sort_index': 0
        },
        {
            'machine_name': 'bio',
            'type': 'integer',
            'name': 'Teaching Experience',
            'value': 123,
            'sort_index': 0.123
        },
        {
            'machine_name': 'date',
            'type': 'date',
            'name': 'The Date',
            'value': '2032-06-01',
            'sort_index': 0.123
        }
    ]
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
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
    assert result.extra_attributes == [{k: v for k, v in ea_.items() if k != 'sort_index'} for ea_ in eas]
    assert result.tag_line is None
    assert result.primary_description is None

    r = await cli.get(cli.server.app.router['contractor-get'].url_for(company='thepublickey', id='123', slug='x'))
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['id'] == 123
    assert len(obj['extra_attributes']) == 3
    assert obj['extra_attributes'][2]['value'] == '2032-06-01'


async def test_extra_attributes_special(cli, db_conn, company):
    eas = [
        {
            'machine_name': 'tag_line_a',
            'type': 'checkbox',
            'name': 'Should be missed',
            'value': True,
            'sort_index': 0
        },
        {
            'machine_name': 'whatever',
            'type': 'text_short',
            'name': 'Should be missed',
            'value': 'whatever',
            'sort_index': 0
        },
        {
            'machine_name': 'tag_line',
            'type': 'text_short',
            'name': 'Should be used',
            'value': 'this is the tag line',
            'sort_index': 10
        },
        {
            'machine_name': 'foobar',
            'type': 'text_extended',
            'name': 'Primary Description',
            'value': 'Should be used as primary description',
            'sort_index': 1
        },
        {
            'machine_name': 'no_primary',
            'type': 'text_extended',
            'name': 'Not Primary Description',
            'value': 'Should not be used as primary description because it has a higher sort index than above',
            'sort_index': 2
        }
    ]
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
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
    assert [ea['machine_name'] for ea in result.extra_attributes] == ['tag_line_a', 'whatever', 'no_primary']


async def test_extra_attributes_null(cli, db_conn, company):
    eas = [
        {
            'machine_name': 'terms',
            'type': 'checkbox',
            'name': 'Terms and Conditions agreement',
            'value': None,
            'id': 381,
            'sort_index': 0
        }
    ]
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
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
    assert result.extra_attributes == []
    assert result.tag_line is None
    assert result.primary_description is None


@pytest.mark.parametrize('image_format', ['JPEG', 'RGBA', 'P'])
async def test_photo(cli, db_conn, company, image_download_url, tmpdir, other_server, image_format):
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
        id=123,
        first_name='Fred',
        photo=f'{image_download_url}?format={image_format}'
    )
    assert r.status == 201, await r.text()
    assert other_server.app['request_log'] == [('test_image', image_format)]

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
    r = await signed_request(cli, f'/{company.public_key}/webhook/contractor', id=123, first_name='Fred')
    assert r.status == 201
    assert [cs.first_name async for cs in await db_conn.execute(sa_contractors.select())] == ['Fred']

    r = await signed_request(cli, f'/{company.public_key}/webhook/contractor', id=123, first_name='George')
    assert r.status == 200
    assert [cs.first_name async for cs in await db_conn.execute(sa_contractors.select())] == ['George']


async def test_photo_hash(cli, db_conn, company, image_download_url, tmpdir):
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
        id=123,
        first_name='Fred',
    )
    assert r.status == 201, await r.text()

    cons = sorted([(cs.first_name, cs.photo_hash) async for cs in await db_conn.execute(sa_contractors.select())])
    assert cons == [('Fred', '-')]

    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
        id=124,
        first_name='George',
        photo=f'{image_download_url}?format=JPEG'
    )
    assert r.status == 201, await r.text()

    path = Path(tmpdir / 'media' / company.public_key / '124.thumb.jpg')
    assert path.exists()

    cons = sorted([(cs.first_name, cs.photo_hash) async for cs in await db_conn.execute(sa_contractors.select())])
    assert cons == [('Fred', '-'), ('George', hashlib.md5(path.read_bytes()).hexdigest()[:6])]


async def test_delete(cli, db_conn, company):
    assert 0 == await count(db_conn, sa_contractors)
    r = await signed_request(cli, f'/{company.public_key}/webhook/contractor', id=123, first_name='Fred')
    assert r.status == 201
    assert 1 == await count(db_conn, sa_contractors)

    r = await signed_request(cli, f'/{company.public_key}/webhook/contractor', id=123, deleted=True)
    assert r.status == 200
    assert 0 == await count(db_conn, sa_contractors)

    r = await signed_request(cli, f'/{company.public_key}/webhook/contractor', id=123, deleted=True)
    assert r.status == 404
    assert 0 == await count(db_conn, sa_contractors)


async def test_delete_all_fields(cli, db_conn, company):
    assert 0 == await count(db_conn, sa_contractors)
    r = await signed_request(cli, f'/{company.public_key}/webhook/contractor', id=123, first_name='Fred')
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

    r = await signed_request(cli, f'/{company.public_key}/webhook/contractor', **data)
    assert r.status == 200, await r.text()
    assert 0 == await count(db_conn, sa_contractors)

    r = await signed_request(cli, f'/{company.public_key}/webhook/contractor', id=123, deleted=True)
    assert r.status == 404
    assert 0 == await count(db_conn, sa_contractors)


async def test_delete_skills(cli, db_conn, company):
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
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

    r = await signed_request(cli, f'/{company.public_key}/webhook/contractor', id=123, deleted=True)
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
    r = await cli.post(f'/{company.public_key}/webhook/contractor', data=payload, headers=headers)
    assert r.status == 400, await r.text()
    response_data = await r.json()
    assert response_data == {
        'details': 'Value Error: Expecting value: line 1 column 1 (char 0)',
        'status': 'invalid request data',
    }


async def test_invalid_schema(cli, company):
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
        id='not an int',
    )
    assert r.status == 400, await r.text()
    response_data = await r.json()
    assert response_data == {
        'details': {
            'id': {
                'error_msg': "invalid literal for int() with base 10: 'not an int'",
                'error_type': 'ValueError',
                'track': 'int'
            }
        },
        'status': 'invalid request data',
    }


async def test_missing_company(cli, company):
    r = await signed_request(
        cli,
        f'/not-{company.public_key}/webhook/contractor',
        id=123,
    )
    assert r.status == 404, await r.text()
    response_data = await r.json()
    assert response_data == {
        'details': 'No company found for key not-thepublickey',
        'status': 'company not found',
    }


async def test_invalid_input(cli, db_conn, company):
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
        id=123,
        first_name='x' * 100,
    )
    assert r.status == 400, await r.text()
    data = await r.json()
    assert {
        'details': {
            'first_name': {
                'error_msg': 'length greater than maximum allowed: 63',
                'error_type': 'ValueError',
                'track': 'ConstrainedStrValue',
            },
        },
        'status': 'invalid request data',
    } == data


async def test_create_labels(cli, db_conn, company):
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
        id=123,
        first_name='Fred',
        labels=[
            {
                'machine_name': 'foobar',
                'name': 'Foobar',
            },
            {
                'machine_name': 'apple-pie',
                'name': 'Apple Pie',
            },
        ]
    )
    assert r.status == 201, await r.text()
    labels = await select_set(db_conn, sa_labels.c.machine_name, sa_labels.c.name, sa_labels.c.company)
    assert labels == {('apple-pie', 'Apple Pie', company.id), ('foobar', 'Foobar', company.id)}

    con = await get(db_conn, sa_contractors, sa_contractors.c.id == 123)
    assert con['labels'] == ['foobar', 'apple-pie']


async def test_delete_all_labels(cli, db_conn, company):
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
        id=123,
        labels=[
            {
                'machine_name': 'foobar',
                'name': 'Foobar',
            },
        ]
    )
    assert r.status == 201, await r.text()
    assert 1 == await count(db_conn, sa_contractors)
    assert 1 == await count(db_conn, sa_labels)
    con = await get(db_conn, sa_contractors, sa_contractors.c.id == 123)
    assert con['labels'] == ['foobar']

    r = await signed_request(cli, f'/{company.public_key}/webhook/contractor', id=123)
    assert r.status == 200
    assert 1 == await count(db_conn, sa_contractors)
    assert 1 == await count(db_conn, sa_labels)
    con = await get(db_conn, sa_contractors, sa_contractors.c.id == 123)
    assert con['labels'] == []


async def test_delete_some_labels(cli, db_conn, company):
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
        id=123,
        labels=[
            {
                'machine_name': 'foobar',
                'name': 'Foobar',
            },
        ]
    )
    assert r.status == 201, await r.text()
    labels = await select_set(db_conn, sa_labels.c.machine_name, sa_labels.c.name)
    assert labels == {('foobar', 'Foobar')}
    con = await get(db_conn, sa_contractors, sa_contractors.c.id == 123)
    assert con['labels'] == ['foobar']

    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
        id=123,
        labels=[
            {
                'machine_name': 'squiggle',
                'name': 'Squiggle',
            },
        ]
    )
    assert r.status == 200, await r.text()

    labels = await select_set(db_conn, sa_labels.c.machine_name, sa_labels.c.name)
    assert labels == {('squiggle', 'Squiggle'), ('foobar', 'Foobar')}
    con = await get(db_conn, sa_contractors, sa_contractors.c.id == 123)
    assert con['labels'] == ['squiggle']


async def test_labels_conflict(cli, db_conn, company):
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
        id=123,
        labels=[
            {
                'machine_name': 'foobar',
                'name': 'Foobar',
            },
        ]
    )
    assert r.status == 201, await r.text()
    labels = await select_set(db_conn, sa_labels.c.machine_name, sa_labels.c.name)
    assert labels == {('foobar', 'Foobar')}
    label_ids = await select_set(db_conn, sa_labels.c.id)

    con = await get(db_conn, sa_contractors, sa_contractors.c.id == 123)
    assert con['labels'] == ['foobar']

    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
        id=123,
        labels=[
            {
                'machine_name': 'foobar',
                'name': 'Squiggle',
            },
        ]
    )
    assert r.status == 200, await r.text()

    labels = await select_set(db_conn, sa_labels.c.machine_name, sa_labels.c.name)
    assert labels == {('foobar', 'Squiggle')}

    con = await get(db_conn, sa_contractors, sa_contractors.c.id == 123)
    assert con['labels'] == ['foobar']

    assert label_ids == await select_set(db_conn, sa_labels.c.id)


async def test_add_review_info(cli, db_conn, company):
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
        signing_key_='this is the master key',
        id=321,
        review_rating=3.5,
        review_duration=7200,
    )
    assert r.status == 201, await r.text()
    curr = await db_conn.execute(sa_contractors.select())
    result = await curr.first()
    assert result.id == 321
    assert result.review_rating == 3.5
    assert result.review_duration == 7200
    assert result.latitude is None
    assert result.longitude is None


async def test_add_location(cli, db_conn, company):
    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/contractor',
        signing_key_='this is the master key',
        id=321,
        location=dict(
            latitude=12.345,
            longitude=56.789,
        )
    )
    assert r.status == 201, await r.text()
    curr = await db_conn.execute(sa_contractors.select())
    result = await curr.first()
    assert result.id == 321
    assert result.review_rating is None
    assert result.review_duration == 0
    assert result.latitude == 12.345
    assert result.longitude == 56.789
