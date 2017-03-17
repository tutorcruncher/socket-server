import asyncio
import json
from pathlib import Path
from tempfile import TemporaryFile
from urllib.parse import urlencode

from aiohttp import ClientSession
from aiopg.sa import create_engine
from arq import Actor, BaseWorker, RedisSettings, concurrent
from arq.utils import timestamp
from PIL import Image, ImageOps
from psycopg2 import OperationalError

from .logs import logger
from .processing import contractor_set
from .settings import load_settings, pg_dsn
from .views import VIEW_SCHEMAS

CHUNK_SIZE = int(1e4)
SIZE_LARGE = 1000, 1000
SIZE_SMALL = 256, 256

CT_JSON = 'application/json'


class MainActor(Actor):
    def __init__(self, *, settings=None, **kwargs):
        self.settings = settings or load_settings()
        kwargs['redis_settings'] = RedisSettings(**self.settings['redis'])
        super().__init__(**kwargs)
        self.api_root = self.settings['tc_api_root']
        self.api_contractors = self.api_root + '/contractors/'
        self.api_enquiries = self.api_root + '/enquiry/'
        self.session = self.media = self.pg_engine = None

    async def startup(self, retries=5):
        if self.session and self.media and self.pg_engine:
            # happens if startup is called twice eg. in test setup
            return
        try:
            self.pg_engine = await create_engine(pg_dsn(self.settings['database']), loop=self.loop)
        except OperationalError:
            if retries > 0:
                logger.info('create_engine failed, %d retries remaining, retrying...', retries)
                await asyncio.sleep(1, loop=self.loop)
                return await self.startup(retries=retries - 1)
            else:
                raise
        else:
            self.session = ClientSession(loop=self.loop)
            self.media = Path(self.settings['media_dir'])

    @concurrent
    async def get_image(self, company, contractor_id, url):
        save_dir = self.media / company
        save_dir.mkdir(exist_ok=True)
        path_str = str(save_dir / str(contractor_id))
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
                img_thumb = ImageOps.fit(img, SIZE_LARGE, Image.LANCZOS)
                img_thumb.save(path_str + '.jpg', 'JPEG')

                img_large = ImageOps.fit(img, SIZE_SMALL, Image.LANCZOS)
                img_large.save(path_str + '.thumb.jpg', 'JPEG')
        return 200

    def request_headers(self, company):
        return dict(accept=CT_JSON, authorization=f'Token {company["private_key"]}')

    async def _get_from_api(self, url, schema, company):
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
                    yield schema.check(con_data)

                url = response_data.get('next')

            if not url:
                break

    async def _get_cons(self, company):
        async for r in self._get_from_api(self.api_contractors, VIEW_SCHEMAS['contractor-set'], company):
            yield r

    @concurrent(Actor.LOW_QUEUE)
    async def update_contractors(self, company):
        # TODO: delete existing contractors
        cons_created = 0
        async with self.pg_engine.acquire() as conn:
            async for con_data in self._get_cons(company):
                await contractor_set(
                    conn=conn,
                    worker=self,
                    company=company,
                    data=con_data,
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
        redis_pool = await self.get_redis_pool()
        async with redis_pool.get() as redis:
            await redis.setex(b'enquiry-data-%d' % company['id'], 3600, json.dumps(data).encode())

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
        data = dict(
            secret=self.settings['grecaptcha_secret'],
            response=grecaptcha_response,
        )
        if client_ip:
            data['remoteip'] = client_ip
        data = urlencode(data).encode()
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        async with self.session.post(self.settings['grecaptcha_url'], data=data, headers=headers) as r:
            assert r.status == 200
            obj = await r.json()
            domain = company['domain']
            if obj['success'] is True and (domain is None or obj['hostname'].endswith(domain)):
                return True
            else:
                logger.warning('google recaptcha failure, response: %s', obj)

    @concurrent
    async def submit_enquiry(self, company, data):
        grecaptcha_response = data.pop('grecaptcha_response')
        if not await self._check_grecaptcha(company, grecaptcha_response, data['ip_address']):
            return
        data_enc = json.dumps(data)
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
        if r.status == 400:
            logger.warning('400 response submitting enquiry\nrequest: %s\nresponse: %s', data, response_data)
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
        kwargs['redis_settings'] = RedisSettings(**load_settings()['redis'])
        super().__init__(**kwargs)
