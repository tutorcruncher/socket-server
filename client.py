#!/usr/bin/env python3.6
import asyncio
import json
import os
import hmac
import hashlib
from datetime import datetime, timedelta

import aiohttp
import click

SIGNING_KEY = os.getenv('CLIENT_SIGNING_KEY', 'testing').encode()
BASE_URL = os.getenv('CLIENT_BASE_URL', 'http://localhost:8000/')
print(f'using shared secret {SIGNING_KEY} and url {BASE_URL}')
# BASE_URL = 'https://socket.tutorcruncher.com/'
CONN = aiohttp.TCPConnector(verify_ssl=False)

commands = []


def command(func):
    commands.append(func)
    return func


@command
async def index(**kwargs):
    async with aiohttp.ClientSession(connector=CONN) as session:
        async with session.get(BASE_URL) as r:
            print(f'status: {r.status}')
            text = await r.text()
            print(f'response: {text}')


@command
async def company_list(**kwargs):
    payload = (datetime.now() - timedelta(seconds=1)).strftime('%s')
    b_payload = payload.encode()
    m = hmac.new(SIGNING_KEY, b_payload, hashlib.sha256)

    headers = {
        'Signature': m.hexdigest(),
        'Request-Time': payload,
    }
    async with aiohttp.ClientSession(connector=CONN) as session:
        async with session.get(BASE_URL + 'companies', headers=headers) as r:
            print(f'status: {r.status}')
            text = await r.text()
            print(f'response: {text}')


@command
async def company_create(*, public_key=None, data=None, **kwargs):
    post_data = {
        'name': f'company {datetime.now():%y-%m-%d %H:%M:%S}',
        'public_key': public_key,
    }
    data and post_data.update(data)
    payload = json.dumps(post_data)
    b_payload = payload.encode()
    m = hmac.new(SIGNING_KEY, b_payload, hashlib.sha256)
    headers = {
        'Webhook-Signature': m.hexdigest(),
        'User-Agent': 'TutorCruncher',
        'Content-Type': 'application/json',
    }

    async with aiohttp.ClientSession(connector=CONN) as session:
        async with session.post(BASE_URL + 'companies/create', data=payload, headers=headers) as r:
            print(f'status: {r.status}')
            text = await r.text()
            print(f'response: {text}')


@command
async def company_update(*, public_key, data, **kwargs):
    payload = json.dumps(data)
    b_payload = payload.encode()
    m = hmac.new(SIGNING_KEY, b_payload, hashlib.sha256)
    headers = {
        'Webhook-Signature': m.hexdigest(),
        'User-Agent': 'TutorCruncher',
        'Content-Type': 'application/json',
    }
    async with aiohttp.ClientSession(connector=CONN) as session:
        async with session.post(BASE_URL + f'{public_key}/update', data=payload, headers=headers) as r:
            print(f'status: {r.status}')
            text = await r.text()
            print(f'response: {text}')


CON_DATA = {
    'id': 23502,
    'deleted': False,
    'first_name': 'Gerry',
    'last_name': 'Howell',
    'town': 'Edinburgh',
    'country': 'United Kingdom',
    'location': {
        'latitude': None,
        'longitude': None
    },
    'photo': 'http://unsplash.com/photos/vltMzn0jqsA/download',
    'extra_attributes': [
        {
            'machine_name': None,
            'name': 'Bio',
            'type': 'text_extended',
            'sort_index': 0,
            'value': 'The returned group is itself an iterator that shares the underlying iterable with groupby(). '
                     'Because the source is shared, when the groupby() object is advanced, the previous group is no '
                     'longer visible. So, if that data is needed later, it should be stored as a list:',
            'id': 195
        },
        {
            'machine_name': None,
            'name': 'Teaching Experience',
            'type': 'text_short',
            'sort_index': 0,
            'value': 'Harvard',
            'id': 196
        },
    ],
    'skills': [
        {
            'qual_level': 'A Level',
            'subject': 'Mathematics',
            'qual_level_ranking': 18.0,
            'category': 'Maths'
        },
        {
            'qual_level': 'GCSE',
            'subject': 'Mathematics',
            'qual_level_ranking': 16.0,
            'category': 'Maths'
        },
        {
            'qual_level': 'GCSE',
            'subject': 'Algebra',
            'qual_level_ranking': 16.0,
            'category': 'Maths'
        },
        {
            'qual_level': 'KS3',
            'subject': 'Language',
            'qual_level_ranking': 13.0,
            'category': 'English'
        },
        {
            'qual_level': 'Degree',
            'subject': 'Mathematics',
            'qual_level_ranking': 21.0,
            'category': 'Maths'
        },
    ],
    'labels': [],
    'last_updated': '2017-01-08T12:20:46.244Z',
    'created': '2015-01-19',
    'release_timestamp': '2017-01-08T12:27:07.541165Z'
}


@command
async def contractor_create(*, public_key, **kwargs):
    payload = json.dumps(CON_DATA)
    b_payload = payload.encode()
    m = hmac.new(SIGNING_KEY, b_payload, hashlib.sha256)
    headers = {
        'Webhook-Signature': m.hexdigest(),
        'User-Agent': 'TutorCruncher',
        'Content-Type': 'application/json',
    }
    async with aiohttp.ClientSession(connector=CONN) as session:
        async with session.post(BASE_URL + f'{public_key}/contractors/set', data=payload, headers=headers) as r:
            print(f'status: {r.status}')
            text = await r.text()
            print(f'response: {text}')


@command
async def contractor_list(*, public_key, **kwargs):
    async with aiohttp.ClientSession(connector=CONN) as session:
        async with session.get(BASE_URL + f'{public_key}/contractors') as r:
            print(f'status: {r.status}')
            text = await r.text()
            print(f'response: {text}')


@command
async def submit_enquiry(*, public_key, **kwargs):
    async with aiohttp.ClientSession(connector=CONN) as session:
        data = {
            'client_name': 'Cat Flap',
            'client_phone': '123',
            'grecaptcha_response': ,
        }
        headers = {
            'User-Agent': 'Testing Browser',
            'Referer': 'X' * 2000,
        }
        async with session.post(BASE_URL + f'{public_key}/enquiry', data=json.dumps(data), headers=headers) as r:
            print(f'status: {r.status}')
            text = await r.text()
            print(f'response: {text}')


missing = object()


@click.command()
@click.argument('command', type=click.Choice([c.__name__ for c in commands]))
@click.option('-p', '--public-key', default=missing)
@click.option('-d', '--data', default=missing)
def cli(command, **kwargs):
    command_lookup = {c.__name__: c for c in commands}

    kwargs = {k: v for k, v in kwargs.items() if v != missing}
    kwargs['data'] = kwargs.get('data') and json.loads(kwargs.get('data'))

    func = command_lookup[command]
    print(f'running {func.__name__}, kwargs = {kwargs}...')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(func(**kwargs))


if __name__ == '__main__':
    cli()
