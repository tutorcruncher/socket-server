import asyncio
import hashlib
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryFile
from urllib.parse import urlencode

from aiohttp import ClientSession
from aiopg.sa import create_engine
from arq import Actor, BaseWorker, concurrent, cron
from arq.utils import timestamp
from PIL import Image, ImageOps
from psycopg2 import OperationalError
from sqlalchemy import update

from .middleware import domain_allowed
from .models import sa_appointments, sa_contractors
from .processing import contractor_set
from .settings import Settings
from .validation import ContractorModel

CHUNK_SIZE = int(1e4)
SIZE_LARGE = 1000, 1000
SIZE_SMALL = 256, 256
REDIS_ENQUIRY_CACHE_KEY = b'enquiry-data-%d'

CT_JSON = 'application/json'
logger = logging.getLogger('socket.worker')


async def store_enquiry_data(redis, company, data):
    await redis.setex(REDIS_ENQUIRY_CACHE_KEY % company['id'], 86400, json.dumps(data).encode())


class MainActor(Actor):
    def __init__(self, *, settings: Settings=None, **kwargs):
        self.settings = settings or Settings()
        self.redis_settings = self.settings.redis_settings
        super().__init__(**kwargs)
        self.api_root = self.settings.tc_api_root
        self.api_contractors = self.api_root + '/contractors/'
        self.api_enquiries = self.api_root + '/enquiry/'
        self.api_book_appointment = self.api_root + '/recipients/'
        self.session = self.media = self.pg_engine = None
        self.retry_sleep = 1

    async def startup(self, retries=5):
        if self.session and self.media and self.pg_engine:
            # happens if startup is called twice eg. in test setup
            return
        try:
            self.pg_engine = await create_engine(self.settings.pg_dsn, loop=self.loop)
        except OperationalError:
            if retries > 0:
                logger.info('create_engine failed, %d retries remaining, retrying...', retries)
                await asyncio.sleep(self.retry_sleep, loop=self.loop)
                return await self.startup(retries=retries - 1)
            else:
                raise
        else:
            logger.info('db engine created successfully')
            self.session = ClientSession(loop=self.loop)
            self.media = Path(self.settings.media_dir)

    @concurrent
    async def get_image(self, company, contractor_id, url):
        save_dir = self.media / company
        save_dir.mkdir(exist_ok=True)
        image_path_main = save_dir / f'{contractor_id}.jpg'
        image_path_thumb = save_dir / f'{contractor_id}.thumb.jpg'
        with TemporaryFile() as f:
            async with self.session.get(url) as r:
                if r.status != 200:
                    logger.warning('company %s, contractor %d, unable to download %s: %d',
                                   company, contractor_id, url, r.status)
                    return r.status
                while True:
                    chunk = await r.content.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)

            save_image(f, image_path_main, image_path_thumb)

        image_hash = hashlib.md5(image_path_thumb.read_bytes()).hexdigest()
        async with self.pg_engine.acquire() as conn:
            await conn.execute(
                update(sa_contractors)
                .values(photo_hash=image_hash[:6])
                .where(sa_contractors.c.id == contractor_id)
            )
        return 200

    def request_headers(self, company, extra=None):
        return dict(accept=CT_JSON, authorization=f'Token {company["private_key"]}', **(extra or {}))

    async def _get_from_api(self, url, model, company):
        headers = self.request_headers(company)
        while True:
            async with self.session.get(url, headers=headers) as r:
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

    @concurrent(Actor.LOW_QUEUE)
    async def update_contractors(self, company):
        # TODO: delete existing contractors
        cons_created = 0
        async with self.pg_engine.acquire() as conn:
            async for contractor in self._get_from_api(self.api_contractors, ContractorModel, company):

                await contractor_set(
                    conn=conn,
                    worker=self,
                    company=company,
                    contractor=contractor,
                    skip_deleted=True,
                )
                cons_created += 1
        return cons_created

    @concurrent
    async def update_enquiry_options(self, company):
        """
        update the redis key containing enquiry option data, including setting the "last_updated" key.
        """
        data = await self.get_enquiry_options(company)
        data['last_updated'] = timestamp()
        redis = await self.get_redis()
        await store_enquiry_data(redis, company, data)

    async def get_enquiry_options(self, company):
        async with self.session.options(self.api_enquiries, headers=self.request_headers(company)) as r:
            try:
                assert r.status == 200
                response_data = await r.json()
            except (ValueError, AssertionError) as e:
                body = await r.read()
                raise RuntimeError(f'Bad response from {self.api_enquiries} {r.status}, response:\n{body}') from e
        data = response_data['actions']['POST']
        # these are set by socket-server itself
        for f in ('user_agent', 'ip_address', 'http_referrer'):
            data.pop(f)
        return data

    async def _check_grecaptcha(self, company, grecaptcha_response, client_ip):
        if grecaptcha_response == 'mock-grecaptcha:{[private_key]}'.format(company):
            logger.info('skipping recaptcha using company private key')
            return True
        data = dict(
            secret=self.settings.grecaptcha_secret,
            response=grecaptcha_response,
        )
        if client_ip:
            data['remoteip'] = client_ip
        data = urlencode(data).encode()
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        async with self.session.post(self.settings.grecaptcha_url, data=data, headers=headers) as r:
            assert r.status == 200
            obj = await r.json()
            domains = company['domains']
            if obj['success'] is True and (domains is None or domain_allowed(domains, obj['hostname'])):
                return True
            else:
                logger.warning('google recaptcha failure, response: %s', obj)

    @concurrent
    async def submit_enquiry(self, company, data):
        grecaptcha_response = data.pop('grecaptcha_response')
        if not await self._check_grecaptcha(company, grecaptcha_response, data['ip_address']):
            return
        status = await self.post_data(self.api_enquiries, data, company)
        if status != 200:
            await self.update_enquiry_options(company)
        return status

    @concurrent
    async def submit_booking(self, company, data):
        return await self.post_data(self.api_book_appointment, data, company)

    async def post_data(self, url, data, company):
        data_enc = json.dumps(data)
        logger.info('POST => %s %s', url, data_enc)
        headers = self.request_headers(company, {'Content-Type': CT_JSON})
        async with self.session.post(url, data=data_enc, headers=headers) as r:
            response_data = await r.read()
        response_data = response_data.decode()
        logger.info('%s: response: %d, %s', url, r.status, response_data)
        if r.status not in {200, 201}:
            logger.error('%d response posting to %s', r.status, url, extra={
                'data': {
                    'company': company,
                    'request_headers': headers,
                    'request_url': url,
                    'request_data': data,
                    'response_headers': dict(r.headers),
                    'response_data': response_data,
                }
            })
        return r.status

    @cron(hour=3, minute=0)
    async def delete_old_appointments(self):
        async with self.pg_engine.acquire() as conn:
            old = datetime.utcnow() - timedelta(days=7)
            v = await conn.execute(
                sa_appointments.delete()
                .where(sa_appointments.c.start < old)
            )
            logger.info('%d old appointments deleted', v.rowcount)

    async def shutdown(self):
        if self.pg_engine:
            self.pg_engine.close()
            await self.pg_engine.wait_closed()
        if self.session:
            await self.session.close()


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
                rotation = rotations.get(exif[exif_orientation])
                if rotation:
                    img = img.rotate(rotation, expand=True)

        img = img.convert('RGB')
        img_large = ImageOps.fit(img, SIZE_LARGE, Image.LANCZOS)
        img_large.save(image_path_main, 'JPEG')

        img_thumb = ImageOps.fit(img, SIZE_SMALL, Image.LANCZOS)
        img_thumb.save(image_path_thumb, 'JPEG')


class Worker(BaseWorker):
    shadows = [MainActor]

    def __init__(self, **kwargs):  # pragma: no cover
        kwargs['redis_settings'] = Settings().redis_settings
        super().__init__(**kwargs)
