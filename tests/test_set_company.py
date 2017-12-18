import hashlib
import hmac
import json
from datetime import datetime, timedelta

import pytest

from tcsocket.app.models import sa_companies, sa_contractors

from .conftest import signed_post


async def test_create(cli, db_conn):
    payload = json.dumps({'name': 'foobar'})
    b_payload = payload.encode()
    m = hmac.new(b'this is the master key', b_payload, hashlib.sha256)

    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    r = await cli.post('/companies/create', data=payload, headers=headers)
    assert r.status == 201, await r.text()
    response_data = await r.json()
    curr = await db_conn.execute(sa_companies.select())
    result = await curr.first()
    assert result.name == 'foobar'
    assert response_data == {
        'details': {
            'name': 'foobar',
            'public_key': result.public_key,
            'private_key': result.private_key,
        },
        'status': 'success'
    }


async def test_create_with_url_public_key(cli, db_conn):
    payload = json.dumps({
        'name': 'foobar',
        'url': 'https://www.example.com',
        'public_key': 'X' * 20,
    })
    b_payload = payload.encode()
    m = hmac.new(b'this is the master key', b_payload, hashlib.sha256)

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
    assert result.public_key == 'X' * 20
    assert result.domain == 'example.com'
    assert response_data == {
        'details': {
            'name': 'foobar',
            'public_key': 'X' * 20,
            'private_key': result.private_key,
        },
        'status': 'success'
    }


async def test_create_with_keys(cli, db_conn):
    data = {'name': 'foobar', 'public_key': 'x' * 20, 'private_key': 'y' * 40}
    payload = json.dumps(data)
    b_payload = payload.encode()
    m = hmac.new(b'this is the master key', b_payload, hashlib.sha256)

    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    r = await cli.post('/companies/create', data=payload, headers=headers)
    assert r.status == 201
    curr = await db_conn.execute(sa_companies.select())
    result = await curr.first()
    assert result.name == 'foobar'
    assert [(cs.id, cs.first_name, cs.last_name) async for cs in await db_conn.execute(sa_contractors.select())] == [
        (22, 'James', 'Higgins'), (23, None, 'Person 2')
    ]


async def test_create_not_auth(cli):
    headers = {'Content-Type': 'application/json'}
    r = await cli.post('/companies/create', data=json.dumps({'name': 'foobar'}), headers=headers)
    assert r.status == 401


async def test_create_bad_auth(cli):
    payload = json.dumps({'name': 'foobar'})
    b_payload = payload.encode()
    m = hmac.new(b'this is the master key', b_payload, hashlib.sha256)

    headers = {
        'Webhook-Signature': m.hexdigest() + '1',
        'Content-Type': 'application/json',
    }
    r = await cli.post('/companies/create', data=payload, headers=headers)
    assert r.status == 401


async def test_create_duplicate_name(cli, company):
    r = await signed_post(cli, '/companies/create', name='foobar')
    assert r.status == 400
    response_data = await r.json()
    assert response_data == {'details': 'the supplied data conflicts with an existing company', 'status': 'duplicate'}


async def test_create_duplicate_public_key(cli, db_conn):
    payload = json.dumps({'name': 'foobar', 'public_key': 'x' * 20, 'private_key': 'y' * 40})
    b_payload = payload.encode()
    m = hmac.new(b'this is the master key', b_payload, hashlib.sha256)

    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    r = await cli.post('/companies/create', data=payload, headers=headers)
    assert r.status == 201

    payload = json.dumps({'name': 'foobar 2', 'public_key': 'x' * 20, 'private_key': 'z' * 40})
    b_payload = payload.encode()
    m = hmac.new(b'this is the master key', b_payload, hashlib.sha256)
    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    r = await cli.post('/companies/create', data=payload, headers=headers)
    assert r.status == 400
    response_data = await r.json()
    assert response_data == {'details': 'the supplied data conflicts with an existing company', 'status': 'duplicate'}


async def test_list(cli, company):
    payload = (datetime.now() - timedelta(seconds=2)).strftime('%s')
    b_payload = payload.encode()
    m = hmac.new(b'this is the master key', b_payload, hashlib.sha256)

    headers = {
        'Signature': m.hexdigest(),
        'Request-Time': payload,
    }
    r = await cli.get('/companies', headers=headers)
    assert r.status == 200, await r.text()
    response_data = await r.json()
    assert isinstance(response_data[0].pop('id'), int)
    assert [
        {
            'domain': 'example.com',
            'name': 'foobar',
            'name_display': 'first_name_initial',
            'private_key': 'theprivatekey',
            'public_key': 'thepublickey',
        },
    ] == response_data


@pytest.mark.parametrize('payload_func, name', [
    (lambda: (datetime.now() - timedelta(seconds=12)).strftime('%s'), 'now - 12s'),
    (lambda: (datetime.now() + timedelta(seconds=2)).strftime('%s'), 'now + 2s'),
    (lambda: '10000', 'long long ago'),
    (lambda: '-1', 'just before 1970'),
    (lambda: 'null', 'no time'),
])
async def test_list_invalid_time(cli, company, payload_func, name):
    payload = payload_func()
    b_payload = payload.encode()
    m = hmac.new(b'this is the master key', b_payload, hashlib.sha256)

    headers = {
        'Signature': m.hexdigest(),
        'Request-Time': payload,
    }
    r = await cli.get('/companies', headers=headers)
    assert r.status == 403, await r.text()


async def test_update_company(cli, db_conn, company, other_server):
    curr = await db_conn.execute(sa_companies.select())
    result = await curr.first()
    assert result.domain == 'example.com'
    assert other_server.app['request_log'] == []

    r = await signed_post(
        cli,
        f'/{company.public_key}/update',
        signing_key_='this is the master key',
        url='http://changed.com',
    )
    assert r.status == 200, await r.text()
    response_data = await r.json()
    assert response_data == {
        'details': {'domain': 'changed.com'},
        'company_domain': 'changed.com',
        'status': 'success',
    }
    assert other_server.app['request_log'] == [('contractor_list', None), ('contractor_list', '2')]

    curr = await db_conn.execute(sa_companies.select())
    result = await curr.first()
    assert result.domain == 'changed.com'


async def test_update_company_clear_domain(cli, db_conn, company, other_server):
    curr = await db_conn.execute(sa_companies.select())
    result = await curr.first()
    assert result.domain == 'example.com'
    assert other_server.app['request_log'] == []

    r = await signed_post(
        cli,
        f'/{company.public_key}/update',
        signing_key_='this is the master key',
        url=None,
    )
    assert r.status == 200, await r.text()
    response_data = await r.json()
    assert response_data == {'details': {'domain': None}, 'status': 'success', 'company_domain': None}

    curr = await db_conn.execute(sa_companies.select())
    result = await curr.first()
    assert result.domain is None


async def test_update_company_no_data(cli, db_conn, company, other_server):
    curr = await db_conn.execute(sa_companies.select())
    result = await curr.first()
    assert result.domain == 'example.com'
    assert other_server.app['request_log'] == []

    r = await signed_post(
        cli,
        f'/{company.public_key}/update',
        signing_key_='this is the master key'
    )
    assert r.status == 200, await r.text()
    response_data = await r.json()
    assert response_data == {
        'company_domain': None,
        'details': {
            'domain': None,
        },
        'status': 'success',
    }
