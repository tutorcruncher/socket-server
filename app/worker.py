from pathlib import Path
from tempfile import TemporaryFile

from aiohttp import ClientSession
from arq import Actor, BaseWorker, RedisSettings, concurrent
from PIL import Image

from .settings import load_settings

CHUNK_SIZE = int(1e4)
SIZE_LARGE = 1000, 1000
SIZE_SMALL = 128, 128


class ImageActor(Actor):
    def __init__(self, *, settings=None, **kwargs):
        self.settings = settings or load_settings()
        kwargs['redis_settings'] = RedisSettings(**self.settings['redis'])
        super().__init__(**kwargs)
        self.session = ClientSession(loop=self.loop)
        self.media = Path(self.settings['media'])

    @concurrent
    async def get_image(self, company, contractor_id, url):
        save_dir = self.media / company
        save_dir.mkdir(exist_ok=True)
        path_str = str(save_dir / str(contractor_id))
        with TemporaryFile() as f:
            async with self.session.get(url) as r:
                assert r.status == 200
                while True:
                    chunk = await r.content.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)
            f.seek(0)
            im1 = Image.open(f)
            im2 = im1.copy()

            im1.thumbnail(SIZE_LARGE)
            im1.save(path_str + '.jpg', 'JPEG')

            im2.thumbnail(SIZE_SMALL)
            im2.save(path_str + '.thumb.jpg', 'JPEG')

    async def close(self):
        await super().close()
        await self.session.close()


class Worker(BaseWorker):
    shadows = [ImageActor]

    def __init__(self, **kwargs):
        kwargs['redis_settings'] = RedisSettings(**load_settings()['redis'])
        super().__init__(**kwargs)
