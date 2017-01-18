import hashlib
import hmac
import json
from pathlib import Path

from PIL import Image

from app.models import sa_companies, sa_con_skills, sa_contractors


async def test_create_company(cli, db_conn):
    payload = json.dumps({'name': 'foobar'})
    b_payload = payload.encode()
    m = hmac.new(b'this is the secret key', b_payload, hashlib.sha256)

    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    r = await cli.post('/companies/create', data=payload, headers=headers)
    assert r.status == 201
    response_data = await r.json()
    curr = await db_conn.execute(sa_companies.select())
    result = await curr.first()
    assert result.name == 'foobar'
    assert response_data == {
        'details': {
            'name': 'foobar',
            'key': result.key
        },
        'status': 'success'
    }


async def test_create_company_not_auth(cli):
    headers = {'Content-Type': 'application/json'}
    r = await cli.post('/companies/create', data=json.dumps({'name': 'foobar'}), headers=headers)
    assert r.status == 401


async def test_create_company_bad_auth(cli):
    payload = json.dumps({'name': 'foobar'})
    b_payload = payload.encode()
    m = hmac.new(b'this is the secret key', b_payload, hashlib.sha256)

    headers = {
        'Webhook-Signature': m.hexdigest() + '1',
        'Content-Type': 'application/json',
    }
    r = await cli.post('/companies/create', data=payload, headers=headers)
    assert r.status == 401


async def signed_post(cli, url, **data):
    payload = json.dumps(data)
    b_payload = payload.encode()
    m = hmac.new(b'this is the secret key', b_payload, hashlib.sha256)

    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    return await cli.post(url, data=payload, headers=headers)


async def test_create_duplicate_company(cli, db_conn, company):
    r = await signed_post(cli, '/companies/create', name='foobar')
    assert r.status == 400
    response_data = await r.json()
    assert response_data == {'details': 'company with the name "foobar" already exists', 'status': 'duplicate'}


async def test_create_contractor(cli, db_conn, company):
    r = await signed_post(
        cli,
        f'/{company}/contractors/set',
        id=123,
        deleted=False,
        first_name='Fred',
        last_name='Bloggs',
    )
    response_data = await r.json()
    assert r.status == 201, response_data
    assert response_data == {'details': 'contractor created', 'status': 'success'}
    curr = await db_conn.execute(sa_contractors.select())
    result = await curr.first()
    assert result.id == 123
    assert result.first_name == 'Fred'
    assert result.extra_attributes == []


async def test_create_contractor_bad_auth(cli, company):
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
    r = await cli.post(f'/{company}/contractors/set', data=payload, headers=headers)
    assert r.status == 401


async def test_create_contractor_skills(cli, db_conn, company):
    r = await signed_post(
        cli,
        f'/{company}/contractors/set',
        id=123,
        deleted=False,
        first_name='Fred',
        skills=[
            {
                'qual_level': 'GCSE',
                'subject': 'Algebra',
                'qual_level_ranking': 16.0,
                'category': 'Maths'
            },
            {
                'qual_level': 'GCSE',
                'subject': 'Language',
                'qual_level_ranking': 16.0,
                'category': 'English'
            }
        ]
    )
    assert r.status == 201, await r.text()
    con_skills = [cs async for cs in await db_conn.execute(sa_con_skills.select())]
    assert len(con_skills) == 2
    assert len(set(cs.subject for cs in con_skills)) == 2
    assert len(set(cs.qual_level for cs in con_skills)) == 1
    assert set(cs.contractor for cs in con_skills) == {123}


async def test_create_contractor_extra_attributes(cli, db_conn, company):
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
            'type': 'text_extended',
            'name': 'Teaching Experience',
            'value': 'This is a long field with lots and lots and lots of content.',
            'id': 196,
            'sort_index': 0.123
        }
    ]
    r = await signed_post(
        cli,
        f'/{company}/contractors/set',
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


async def test_create_contractor_photo(cli, db_conn, company, image_download_url, tmpdir):
    r = await signed_post(
        cli,
        f'/{company}/contractors/set',
        id=123,
        first_name='Fred',
        photo=image_download_url
    )
    assert r.status == 201, await r.text()
    assert [cs.first_name async for cs in await db_conn.execute(sa_contractors.select())] == ['Fred']
    path = Path(tmpdir / company / '123.jpg')
    assert path.exists()
    with Image.open(str(path)) as im:
        assert im.size == (1000, 500)
    path = Path(tmpdir / company / '123.thumb.jpg')
    assert path.exists()
    with Image.open(str(path)) as im:
        assert im.size == (128, 64)


async def test_update_contractor(cli, db_conn, company):
    assert [cs.first_name async for cs in await db_conn.execute(sa_contractors.select())] == []
    r = await signed_post(cli, f'/{company}/contractors/set', id=123, first_name='Fred')
    assert r.status == 201
    assert [cs.first_name async for cs in await db_conn.execute(sa_contractors.select())] == ['Fred']

    r = await signed_post(cli, f'/{company}/contractors/set', id=123, first_name='George')
    assert r.status == 200
    assert [cs.first_name async for cs in await db_conn.execute(sa_contractors.select())] == ['George']


async def test_delete_contractor(cli, db_conn, company):
    assert len([cs async for cs in await db_conn.execute(sa_contractors.select())]) == 0
    r = await signed_post(cli, f'/{company}/contractors/set', id=123, first_name='Fred')
    assert r.status == 201
    assert len([cs async for cs in await db_conn.execute(sa_contractors.select())]) == 1

    r = await signed_post(cli, f'/{company}/contractors/set', id=123, deleted=True)
    assert r.status == 200
    assert len([cs async for cs in await db_conn.execute(sa_contractors.select())]) == 0

    r = await signed_post(cli, f'/{company}/contractors/set', id=123, deleted=True)
    assert r.status == 404
    assert len([cs async for cs in await db_conn.execute(sa_contractors.select())]) == 0
