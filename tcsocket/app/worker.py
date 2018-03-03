import asyncio
import hashlib
import json
import logging
from pathlib import Path
from tempfile import TemporaryFile
from urllib.parse import urlencode

from aiohttp import ClientSession
from aiopg.sa import create_engine
from arq import Actor, BaseWorker, concurrent
from arq.utils import timestamp
from PIL import Image, ImageOps
from psycopg2 import OperationalError
from sqlalchemy import update

from .middleware import domain_allowed
from .models import sa_contractors
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
        self.session = self.media = self.pg_engine = None

    async def startup(self, retries=5):
        if self.session and self.media and self.pg_engine:
            # happens if startup is called twice eg. in test setup
            return
        try:
            self.pg_engine = await create_engine(self.settings.pg_dsn, loop=self.loop)
        except OperationalError:
            if retries > 0:
                logger.info('create_engine failed, %d retries remaining, retrying...', retries)
                await asyncio.sleep(1, loop=self.loop)
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
            f.seek(0)
            with Image.open(f) as img:
                img = img.convert('RGB')
                img_large = ImageOps.fit(img, SIZE_LARGE, Image.LANCZOS)
                img_large.save(image_path_main, 'JPEG')

                img_thumb = ImageOps.fit(img, SIZE_SMALL, Image.LANCZOS)
                img_thumb.save(image_path_thumb, 'JPEG')

        image_hash = hashlib.md5(image_path_thumb.read_bytes()).hexdigest()
        async with self.pg_engine.acquire() as conn:
            await conn.execute(
                update(sa_contractors)
                .values(photo_hash=image_hash)
                .where(sa_contractors.c.id == contractor_id)
            )
        return 200

    def request_headers(self, company):
        return dict(accept=CT_JSON, authorization=f'Token {company["private_key"]}')

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
        logger.info('ip: %s', data['ip_address'])
        data_enc = json.dumps(data)
        logger.info('data_enc: %s', data_enc)
        headers = self.request_headers(company)
        headers['Content-Type'] = CT_JSON
        async with self.session.post(self.api_enquiries, data=data_enc, headers=headers) as r:
            try:
                assert r.status in (200, 201, 400)
                response_data = await r.json()
            except (ValueError, AssertionError) as e:
                body = await r.read()
                raise RuntimeError(f'Bad response from {self.api_enquiries} {r.status}, response:\n{body}') from e
        logger.info('Response: %d, %s', r.status, response_data)
        if r.status not in (200, 201):
            logger.error('%d response forwarding enquiry to %s', r.status, self.api_enquiries, extra={
                'data': {
                    'status': r.headers,
                    'company': company,
                    'request': data,
                    'response': response_data,
                }
            })
            await self.update_enquiry_options(company)
        return r.status

    async def shutdown(self):
        if self.pg_engine:
            self.pg_engine.close()
            await self.pg_engine.wait_closed()
        if self.session:
            await self.session.close()


class Worker(BaseWorker):
    shadows = [MainActor]

    def __init__(self, **kwargs):
        kwargs['redis_settings'] = Settings().redis_settings
        super().__init__(**kwargs)
