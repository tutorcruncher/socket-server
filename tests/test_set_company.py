import hashlib
import hmac
import json
from datetime import datetime, timedelta

import pytest

from tcsocket.app.models import sa_companies
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
    assert r.status == 201
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
    response_data = await r.json()
    curr = await db_conn.execute(sa_companies.select())
    result = await curr.first()
    assert result.name == 'foobar'
    assert response_data == {
        'details': data,
        'status': 'success'
    }


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
