import hashlib
import hmac
import json
from pathlib import Path

from PIL import Image
from sqlalchemy import select
from sqlalchemy.sql.functions import count

from app.models import sa_con_skills, sa_contractors, sa_qual_levels, sa_subjects
from .conftest import signed_post


async def test_create(cli, db_conn, company):
    r = await signed_post(
        cli,
        f'/{company}/contractors/set',
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
    r = await cli.post(f'/{company}/contractors/set', data=payload, headers=headers)
    assert r.status == 401


async def test_create_skills(cli, db_conn, company):
    r = await signed_post(
        cli,
        f'/{company}/contractors/set',
        id=123,
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


async def test_modify_skills(cli, db_conn, company):
    r = await signed_post(
        cli,
        f'/{company}/contractors/set',
        id=123,
        skills=[
            {
                'qual_level': 'GCSE',
                'subject': 'Algebra',
                'category': 'Maths'
            },
            {
                'qual_level': 'GCSE',
                'subject': 'Language',
                'category': 'English'
            }
        ]
    )
    assert r.status == 201, await r.text()
    con_skills = [cs async for cs in await db_conn.execute(sa_con_skills.select())]
    assert len(con_skills) == 2
    assert set(cs.contractor for cs in con_skills) == {123}
    assert len(set(cs.subject for cs in con_skills)) == 2
    assert len(set(cs.qual_level for cs in con_skills)) == 1

    r = await signed_post(
        cli,
        f'/{company}/contractors/set',
        id=123,
        skills=[
            {
                'qual_level': 'GCSE',
                'subject': 'Literature',
                'category': 'English'
            }
        ]
    )
    assert r.status == 200, await r.text()
    con_skills = [cs async for cs in await db_conn.execute(sa_con_skills.select())]
    assert len(con_skills) == 1
    assert con_skills[0].contractor == 123

    cur = await db_conn.execute(select([count()]).select_from(sa_subjects))
    assert (await cur.first())[0] == 3
    cur = await db_conn.execute(select([count()]).select_from(sa_qual_levels))
    assert (await cur.first())[0] == 1


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
    assert result.tag_line == 'this is the tag line'
    assert result.primary_description == 'Should be used as primary description'
    assert [ea['id'] for ea in result.extra_attributes] == [1, 2, 5]


async def test_photo(cli, db_conn, company, image_download_url, tmpdir):
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


async def test_update(cli, db_conn, company):
    assert [cs.first_name async for cs in await db_conn.execute(sa_contractors.select())] == []
    r = await signed_post(cli, f'/{company}/contractors/set', id=123, first_name='Fred')
    assert r.status == 201
    assert [cs.first_name async for cs in await db_conn.execute(sa_contractors.select())] == ['Fred']

    r = await signed_post(cli, f'/{company}/contractors/set', id=123, first_name='George')
    assert r.status == 200
    assert [cs.first_name async for cs in await db_conn.execute(sa_contractors.select())] == ['George']


async def test_delete(cli, db_conn, company):
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
