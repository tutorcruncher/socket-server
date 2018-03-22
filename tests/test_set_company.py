import hashlib
import hmac
import json
from datetime import datetime, timedelta
from time import time

import pytest

from tcsocket.app.models import sa_companies, sa_contractors

from .conftest import signed_request


async def test_create(cli, db_conn):
    payload = json.dumps({'name': 'foobar', '_request_time': int(time())})
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
        'domains': ['www.example.com'],
        'public_key': 'X' * 20,
        '_request_time': int(time()),
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
    assert result.domains == ['www.example.com']
    assert response_data == {
        'details': {
            'name': 'foobar',
            'public_key': 'X' * 20,
            'private_key': result.private_key,
        },
        'status': 'success'
    }


async def test_create_with_keys(cli, db_conn):
    data = {'name': 'foobar', 'public_key': 'x' * 20, 'private_key': 'y' * 40, '_request_time': int(time())}
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
    assert {(cs.id, cs.first_name, cs.last_name) async for cs in await db_conn.execute(sa_contractors.select())} == {
        (22, 'James', 'Higgins'), (23, None, 'Person 2')
    }


async def test_create_not_auth(cli):
    data = json.dumps({'name': 'foobar', '_request_time': int(time())})
    headers = {'Content-Type': 'application/json'}
    r = await cli.post('/companies/create', data=data, headers=headers)
    assert r.status == 401


async def test_create_bad_auth(cli):
    payload = json.dumps({'name': 'foobar', '_request_time': int(time())})
    b_payload = payload.encode()
    m = hmac.new(b'this is the master key', b_payload, hashlib.sha256)

    headers = {
        'Webhook-Signature': m.hexdigest() + '1',
        'Content-Type': 'application/json',
    }
    r = await cli.post('/companies/create', data=payload, headers=headers)
    assert r.status == 401


@pytest.mark.parametrize('request_time', [
    lambda: 10,
    lambda: int(time()) - 20,
    lambda: int(time()) + 5,
    lambda: 'foobar'
])
async def test_create_bad_body_time(cli, request_time):
    _request_time = request_time()
    data = {'name': 'foobar', 'public_key': 'x' * 20, 'private_key': 'y' * 40, '_request_time': _request_time}
    payload = json.dumps(data)
    b_payload = payload.encode()
    m = hmac.new(b'this is the master key', b_payload, hashlib.sha256)

    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    r = await cli.post('/companies/create', data=payload, headers=headers)
    assert r.status == 403
    assert {
       'details': f"request time '{_request_time}' not in the last 10 seconds",
       'status': 'invalid request time'
    } == await r.json()


async def test_create_duplicate_name(cli, company):
    r = await signed_request(cli, '/companies/create', name='foobar')
    assert r.status == 409, await r.text()
    response_data = await r.json()
    assert response_data == {'details': 'the supplied data conflicts with an existing company', 'status': 'duplicate'}


async def test_create_duplicate_public_key(cli, db_conn):
    payload = json.dumps({'name': 'foobar', 'public_key': 'x' * 20, 'private_key': 'y' * 40,
                          '_request_time': int(time())})
    b_payload = payload.encode()
    m = hmac.new(b'this is the master key', b_payload, hashlib.sha256)

    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    r = await cli.post('/companies/create', data=payload, headers=headers)
    assert r.status == 201

    payload = json.dumps({'name': 'foobar 2', 'public_key': 'x' * 20, 'private_key': 'z' * 40,
                          '_request_time': int(time())})
    b_payload = payload.encode()
    m = hmac.new(b'this is the master key', b_payload, hashlib.sha256)
    headers = {
        'Webhook-Signature': m.hexdigest(),
        'Content-Type': 'application/json',
    }
    r = await cli.post('/companies/create', data=payload, headers=headers)
    assert r.status == 409, await r.text()
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
            'domains': ['example.com'],
            'name': 'foobar',
            'name_display': 'first_name_initial',
            'private_key': 'theprivatekey',
            'public_key': 'thepublickey',
            'options': None,
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


async def test_default_options(cli, db_conn, company):
    r = await cli.get(f'/{company.public_key}/options')
    assert r.status == 200, await r.text()
    expected = {
        'display_mode': 'grid',
        'name': 'foobar',
        'name_display': 'first_name_initial',
        'pagination': 100,
        'router_mode': 'hash',
        'show_hours_reviewed': True,
        'show_labels': True,
        'show_location_search': True,
        'show_stars': True,
        'show_subject_filter': True,
        'sort_on': 'name',
        'auth_url': None,
    }
    assert expected == await r.json()
    assert (await r.text()).count('\n') > 5

    # with both json encoders
    r = await cli.get(f'/{company.public_key}/options', headers={'Accept': 'application/json'})
    assert r.status == 200, await r.text()
    assert expected == await r.json()
    assert (await r.text()).count('\n') == 0


async def test_update_company(cli, db_conn, company, other_server):
    curr = await db_conn.execute(sa_companies.select())
    result = await curr.first()
    assert result.domains == ['example.com']
    assert other_server.app['request_log'] == []

    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/options',
        signing_key_='this is the master key',
        domains=['changed.com'],
        display_mode='enquiry-modal',
        show_location_search=False,
        pagination=20,
        sort_on='review_rating',
        auth_url='https://foobar.com/whatever',
        distance_units='miles',
        currency={'code': 'GBP', 'symbol': '£'},
    )
    assert r.status == 200, await r.text()
    response_data = await r.json()
    assert response_data == {
        'details': {
            'domains': ['changed.com'],
            'options': {
                'display_mode': 'enquiry-modal',
                'show_location_search': False,
                'pagination': 20,
                'sort_on': 'review_rating',
                'auth_url': 'https://foobar.com/whatever',
                'distance_units': 'miles',
                'currency': {'code': 'GBP', 'symbol': '£'},
            }
        },
        'company_domains': ['changed.com'],
        'status': 'success',
    }
    assert other_server.app['request_log'] == [('contractor_list', None), ('contractor_list', '2')]

    curr = await db_conn.execute(sa_companies.select())
    result = await curr.first()
    assert result.domains == ['changed.com']
    assert result.options == {
        'display_mode': 'enquiry-modal',
        'pagination': 20,
        'show_location_search': False,
        'sort_on': 'review_rating',
        'auth_url': 'https://foobar.com/whatever',
        'distance_units': 'miles',
        'currency': {'code': 'GBP', 'symbol': '£'},
    }

    r = await cli.get(f'/{company.public_key}/options')
    assert r.status == 200, await r.text()
    assert {
        'display_mode': 'enquiry-modal',
        'name': 'foobar',
        'name_display': 'first_name_initial',
        'pagination': 20,
        'router_mode': 'hash',
        'show_hours_reviewed': True,
        'show_labels': True,
        'show_location_search': False,
        'show_stars': True,
        'show_subject_filter': True,
        'sort_on': 'review_rating',
        'auth_url': 'https://foobar.com/whatever',
    } == await r.json()


async def test_update_company_clear_domain(cli, db_conn, company, other_server):
    curr = await db_conn.execute(sa_companies.select())
    result = await curr.first()
    assert result.domains == ['example.com']
    assert other_server.app['request_log'] == []

    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/options',
        signing_key_='this is the master key',
        domains=None,
    )
    assert r.status == 200, await r.text()
    response_data = await r.json()
    assert response_data == {'details': {'domains': None}, 'status': 'success', 'company_domains': None}

    curr = await db_conn.execute(sa_companies.select())
    result = await curr.first()
    assert result.domains is None


async def test_update_company_no_data(cli, db_conn, company, other_server):
    curr = await db_conn.execute(sa_companies.select())
    result = await curr.first()
    assert result.domains == ['example.com']
    assert other_server.app['request_log'] == []

    r = await signed_request(
        cli,
        f'/{company.public_key}/webhook/options',
        signing_key_='this is the master key',
    )
    assert r.status == 200, await r.text()
    response_data = await r.json()
    assert response_data == {
        'company_domains': ['example.com'],
        'details': {},
        'status': 'success',
    }
