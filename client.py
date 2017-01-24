#!/usr/bin/env python3.6
import asyncio
import json
import os
import hmac
import hashlib
from datetime import datetime

import aiohttp
import click

SHARED_KEY = os.getenv('SHARED_SECRET', 'this is a secret').encode()
BASE_URL = os.getenv('BASE_URL', 'http://localhost:8000/')
print(f'using shared secret {SHARED_KEY} and url {BASE_URL}')
# BASE_URL = 'https://socket.tutorcruncher.com/'
CONN = aiohttp.TCPConnector(verify_ssl=False)

commands = []


def command(func):
    commands.append(func)
    return func


@command
async def index(arg):
    async with aiohttp.ClientSession(connector=CONN) as session:
        async with session.get(BASE_URL) as r:
            print(f'status: {r.status}')
            text = await r.text()
            print(f'response: {text}')


@command
async def create_company(arg):
    data = {
        'name': f'foobar {datetime.now().strftime("%H:%M:%S")}',
        # 'name': f'foobar',
    }
    payload = json.dumps(data)
    b_payload = payload.encode()
    m = hmac.new(SHARED_KEY, b_payload, hashlib.sha256)
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
            'value': 'applecart',
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
            'qual_level_ranking': 16.0,
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
            'qual_level_ranking': 16.0,
            'category': 'English'
        }
    ],
    'labels': [],
    'last_updated': '2017-01-08T12:20:46.244Z',
    'created': '2015-01-19',
    'release_timestamp': '2017-01-08T12:27:07.541165Z'
}


@command
async def create_contractor(company):
    payload = json.dumps(CON_DATA)
    b_payload = payload.encode()
    m = hmac.new(SHARED_KEY, b_payload, hashlib.sha256)
    headers = {
        'Webhook-Signature': m.hexdigest(),
        'User-Agent': 'TutorCruncher',
        'Content-Type': 'application/json',
    }
    async with aiohttp.ClientSession(connector=CONN) as session:
        async with session.post(BASE_URL + f'{company}/contractors/set', data=payload, headers=headers) as r:
            print(f'status: {r.status}')
            text = await r.text()
            print(f'response: {text}')


@command
async def list_contractors(company):
    async with aiohttp.ClientSession(connector=CONN) as session:
        async with session.get(BASE_URL + f'{company}/contractors?sort=thing') as r:
            print(f'status: {r.status}')
            text = await r.text()
            print(f'response: {text}')


@click.command()
@click.argument('command', type=click.Choice([c.__name__ for c in commands]))
@click.argument('arg', required=False)
def cli(command, arg):
    command_lookup = {c.__name__: c for c in commands}

    func = command_lookup[command]
    print(f'running {func.__name__}...')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(func(arg))


if __name__ == '__main__':
    cli()
