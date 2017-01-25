import hashlib
import hmac
import json

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


async def test_create_duplicate(cli, company):
    r = await signed_post(cli, '/companies/create', name='foobar')
    assert r.status == 400
    response_data = await r.json()
    assert response_data == {'details': 'company with the name "foobar" already exists', 'status': 'duplicate'}
