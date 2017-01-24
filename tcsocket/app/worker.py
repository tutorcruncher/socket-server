from pathlib import Path

from tempfile import TemporaryFile

from aiohttp import ClientSession
from arq import Actor, BaseWorker, RedisSettings, concurrent
from PIL import Image, ImageOps

from .logs import logger
from .settings import load_settings

CHUNK_SIZE = int(1e4)
SIZE_LARGE = 1000, 1000
SIZE_SMALL = 256, 256


class ImageActor(Actor):
    def __init__(self, *, settings=None, **kwargs):
        self.settings = settings or load_settings()
        kwargs['redis_settings'] = RedisSettings(**self.settings['redis'])
        super().__init__(**kwargs)
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

    async def close(self):
        await super().close()
        await self.session.close()


class Worker(BaseWorker):
    shadows = [ImageActor]

    def __init__(self, **kwargs):
        kwargs['redis_settings'] = RedisSettings(**load_settings()['redis'])
        super().__init__(**kwargs)
