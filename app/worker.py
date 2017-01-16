from PIL import Image
from tempfile import TemporaryFile
from aiohttp import ClientSession
from arq import Actor, BaseWorker, concurrent

from app.settings import load_settings


CHUNK_SIZE = int(1e4)
SIZE_LARGE = 1000, 1000
SIZE_SMALL = 128, 128


class ImageActor(Actor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.session = ClientSession(loop=self.loop)
        # TODO fix arq to work with a standard settings config
        self.app_settings = load_settings()

    @concurrent
    async def get_image(self, company, contractor_id, url):
        save_dir = self.app_settings['media'] / company
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
        self.session.close()


class Worker(BaseWorker):
    shadows = [ImageActor]
