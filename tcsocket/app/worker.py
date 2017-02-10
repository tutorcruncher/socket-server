from pathlib import Path
from tempfile import TemporaryFile

from aiohttp import ClientSession
from aiopg.sa import create_engine
from arq import Actor, BaseWorker, RedisSettings, concurrent
from PIL import Image, ImageOps

from .logs import logger
from .settings import load_settings, pg_dsn

CHUNK_SIZE = int(1e4)
SIZE_LARGE = 1000, 1000
SIZE_SMALL = 256, 256

CT_JSON = 'application/json'


class RequestActor(Actor):
    def __init__(self, *, settings=None, **kwargs):
        self.settings = settings or load_settings()
        kwargs['redis_settings'] = RedisSettings(**self.settings['redis'])
        super().__init__(**kwargs)
        self.api_root = self.settings['tc_api_root']
        self.api_contractors = self.api_root + '/contractors/'

    async def startup(self):
        self.session = ClientSession(loop=self.loop)
        self.media = Path(self.settings['media_dir'])
        self.pg_engine = await create_engine(pg_dsn(self.settings['database']), loop=self.loop)

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

    async def _get_cons(self, url, **headers):
        async with self.session.get(url, headers=headers) as r:
            print(f'status: {r.status}')
            obj = await r.json()
            print(f'response: {obj}')
        yield 1

    @concurrent(Actor.LOW_QUEUE)
    async def update_contractors(self, public_key, private_key):
        async for con in self._get_cons(self.api_contractors, accept=CT_JSON, authorization='Token ' + private_key):
            print(con)

    async def shutdown(self):
        self.pg_engine.close()
        await self.pg_engine.wait_closed()
        await self.session.close()


class Worker(BaseWorker):
    shadows = [RequestActor]

    def __init__(self, **kwargs):
        kwargs['redis_settings'] = RedisSettings(**load_settings()['redis'])
        super().__init__(**kwargs)
