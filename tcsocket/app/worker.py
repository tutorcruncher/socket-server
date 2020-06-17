import asyncio
import hashlib
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from signal import SIGTERM
from tempfile import TemporaryFile
from urllib.parse import urlencode

from aiohttp import ClientSession
from aiopg.sa import create_engine
from arq import create_pool, cron
from arq.utils import timestamp_ms
from PIL import Image, ImageOps
from psycopg2 import OperationalError
from sqlalchemy import update

from .middleware import domain_allowed
from .models import sa_appointments, sa_contractors
from .processing import contractor_set
from .validation import ContractorModel

CHUNK_SIZE = int(1e4)
SIZE_LARGE = 1000, 1000
SIZE_SMALL = 256, 256
REDIS_ENQUIRY_CACHE_KEY = b'enquiry-data-%d'

CT_JSON = 'application/json'
logger = logging.getLogger('socket')


async def store_enquiry_data(redis, company, data):
    await redis.setex(REDIS_ENQUIRY_CACHE_KEY % company['id'], 86400, json.dumps(data).encode())


async def startup(ctx, retries=5):
    if ctx.get('session') and ctx.get('media') and ctx.get('pg_engine'):
        # happens if startup is called twice eg. in test setup
        return
    try:
        ctx['pg_engine'] = await create_engine(ctx['settings'].pg_dsn)
    except OperationalError:
        if retries > 0:
            logger.info('create_engine failed, %d retries remaining, retrying...', retries)
            await asyncio.sleep(1)
            return await startup(ctx, retries=retries - 1)
        else:
            raise
    else:
        logger.info('db engine created successfully')
        ctx['session'] = ClientSession()
        ctx['media'] = Path(ctx['settings'].media_dir)


async def shutdown(ctx):
    pg_engine = ctx.get('pg_engine')
    if pg_engine:
        pg_engine.close()
        await pg_engine.wait_closed()
    session = ctx.get('session')
    if session:
        await session.close()


async def get_image(ctx, company_key, contractor_id, url):
    save_dir = Path(ctx['settings'].media_dir) / company_key
    save_dir.mkdir(exist_ok=True)
    image_path_main = save_dir / f'{contractor_id}.jpg'
    image_path_thumb = save_dir / f'{contractor_id}.thumb.jpg'
    with TemporaryFile() as f:
        async with ctx['session'].get(url) as r:
            if r.status != 200:
                logger.warning(
                    'company %s, contractor %d, unable to download %s: %d', company_key, contractor_id, url, r.status
                )
                return r.status
            while True:
                chunk = await r.content.read(CHUNK_SIZE)
                if not chunk:
                    break
                f.write(chunk)

        save_image(f, image_path_main, image_path_thumb)

    image_hash = hashlib.md5(image_path_thumb.read_bytes()).hexdigest()
    async with ctx['pg_engine'].acquire() as conn:
        await conn.execute(
            update(sa_contractors).values(photo_hash=image_hash[:6]).where(sa_contractors.c.id == contractor_id)
        )
    return 200


def request_headers(company, extra=None):
    return dict(accept=CT_JSON, authorization=f'Token {company["private_key"]}', **(extra or {}))


async def _get_from_api(session, url, model, company):
    headers = request_headers(company)
    while True:
        async with session.get(url, headers=headers) as r:
            try:
                assert r.status == 200
                response_data = await r.json()
            except (ValueError, AssertionError) as e:
                body = await r.read()
                raise RuntimeError(f'Bad response from {url} {r.status}, response:\n{body}') from e

            for con_data in response_data.get('results') or []:
                yield model.parse_obj(con_data)

            url = response_data.get('next')

        if not url:
            break


async def update_contractors(ctx, company):
    # TODO: delete existing contractors
    cons_created = 0
    api_contractors = ctx['settings'].tc_api_root + ctx['settings'].tc_contractors_endpoint
    async with ctx['pg_engine'].acquire() as conn:
        async for contractor in _get_from_api(ctx['session'], api_contractors, ContractorModel, company):
            await contractor_set(
                conn=conn, ctx=ctx, company=company, contractor=contractor, skip_deleted=True,
            )
            cons_created += 1
    return cons_created


async def get_enquiry_options(ctx, company):
    api_enquiries_url = ctx['settings'].tc_api_root + ctx['settings'].tc_enquiry_endpoint
    async with ctx['session'].options(api_enquiries_url, headers=request_headers(company)) as r:
        try:
            assert r.status == 200
            response_data = await r.json()
        except (ValueError, AssertionError) as e:
            body = await r.read()
            raise RuntimeError(f'Bad response from {api_enquiries_url} {r.status}, response:\n{body}') from e
    data = response_data['actions']['POST']
    # these are set by socket-server itself
    for f in ('user_agent', 'ip_address', 'http_referrer'):
        data.pop(f)
    return data


async def update_enquiry_options(ctx, company):
    """
    update the redis key containing enquiry option data, including setting the "last_updated" key.
    """
    data = await get_enquiry_options(ctx, company)
    data['last_updated'] = timestamp_ms()
    redis = await create_pool(ctx['settings'].redis_settings)
    await store_enquiry_data(redis, company, data)


async def post_data(session, url, data, company):
    data_enc = json.dumps(data)
    logger.info('POST => %s %s', url, data_enc)
    headers = request_headers(company, {'Content-Type': CT_JSON})
    async with session.post(url, data=data_enc, headers=headers) as r:
        response_data = await r.read()
    response_data = response_data.decode()
    logger.info('%s: response: %d, %s', url, r.status, response_data)
    if r.status not in {200, 201}:
        logger.error(
            '%d response posting to %s',
            r.status,
            url,
            extra={
                'data': {
                    'company': company,
                    'request_headers': headers,
                    'request_url': url,
                    'request_data': data,
                    'response_headers': dict(r.headers),
                    'response_data': response_data,
                }
            },
        )
    return r.status


async def submit_enquiry(ctx, company, data):
    api_enquiries_url = ctx['settings'].tc_api_root + ctx['settings'].tc_enquiry_endpoint
    grecaptcha_response = data.pop('grecaptcha_response')
    if not await _check_grecaptcha(ctx['settings'], ctx['session'], company, grecaptcha_response, data['ip_address']):
        return
    status = await post_data(ctx['session'], api_enquiries_url, data, company)
    if status != 200:
        await update_enquiry_options(ctx, company)
    return status


async def _check_grecaptcha(settings, session, company, grecaptcha_response, client_ip):
    if grecaptcha_response == 'mock-grecaptcha:{[private_key]}'.format(company):
        logger.info('skipping recaptcha using company private key')
        return True
    data = dict(secret=settings.grecaptcha_secret, response=grecaptcha_response,)
    if client_ip:
        data['remoteip'] = client_ip
    data = urlencode(data).encode()
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    async with session.post(settings.grecaptcha_url, data=data, headers=headers) as r:
        assert r.status == 200
        obj = await r.json()
        domains = company['domains']
        if obj['success'] is True and (domains is None or domain_allowed(domains, obj['hostname'])):
            return True
        else:
            logger.warning('google recaptcha failure, response: %s', obj)


async def submit_booking(ctx, company, data):
    api_book_appointment_url = ctx['settings'].tc_api_root + ctx['settings'].tc_book_apt_endpoint
    return await post_data(ctx['session'], api_book_appointment_url, data, company)


async def delete_old_appointments(ctx):
    async with ctx['pg_engine'].acquire() as conn:
        old = datetime.utcnow() - timedelta(days=7)
        v = await conn.execute(sa_appointments.delete().where(sa_appointments.c.start < old))
        logger.info('%d old appointments deleted', v.rowcount)


async def kill_worker(ctx):
    pid = os.getppid()
    os.kill(pid, SIGTERM)
    logger.info('Killed worker pid %s nightly', pid)


class WorkerSettings:
    functions = [get_image, submit_booking, submit_enquiry, update_contractors, update_enquiry_options]
    cron_jobs = [
        cron(delete_old_appointments, hour={0, 3, 6, 9, 12, 15, 18, 21}, minute=0),
        cron(kill_worker, hour=3, minute=0),
    ]
    on_startup = startup
    on_shutdown = shutdown


exif_orientation = 0x112
rotations = {
    3: 180,
    6: 270,
    8: 90,
}


def save_image(file, image_path_main, image_path_thumb):
    file.seek(0)
    with Image.open(file) as img:
        # could use more of https://piexif.readthedocs.io/en/latest/sample.html#rotate-image-by-exif-orientation
        if hasattr(img, '_getexif'):
            exif = img._getexif()
            if exif:
                rotation = rotations.get(exif.get(exif_orientation))
                if rotation:
                    img = img.rotate(rotation, expand=True)

        img = img.convert('RGB')
        img_large = ImageOps.fit(img, SIZE_LARGE, Image.LANCZOS)
        img_large.save(image_path_main, 'JPEG')

        img_thumb = ImageOps.fit(img, SIZE_SMALL, Image.LANCZOS)
        img_thumb.save(image_path_thumb, 'JPEG')
