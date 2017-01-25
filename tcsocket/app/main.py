import os
import re

from aiohttp import web
from aiopg.sa import create_engine
from sqlalchemy.engine.url import URL

from .middleware import middleware
from .settings import THIS_DIR, load_settings
from .views import company_create, company_list, contractor_get, contractor_list, contractor_set, index
from .worker import ImageActor


def pg_dsn(db_settings: dict) -> str:
    """
    :param db_settings: dict of connection settings, see SETTINGS_STRUCTURE for definition
    :return: DSN url suitable for sqlalchemy and aiopg.
    """
    return str(URL(
        database=db_settings['name'],
        password=db_settings['password'],
        host=db_settings['host'],
        port=db_settings['port'],
        username=db_settings['user'],
        drivername='postgres',
    ))


async def startup(app: web.Application):
    app.update(
        pg_engine=await create_engine(pg_dsn(app['database']), loop=app.loop),
        image_worker=ImageActor(settings=app['settings']),
    )


async def cleanup(app: web.Application):
    app['pg_engine'].close()
    await app['pg_engine'].wait_closed()
    await app['image_worker'].close()


def setup_routes(app):
    app.router.add_get('/', index, name='index')
    app.router.add_post('/companies/create', company_create, name='company-create')
    app.router.add_get('/companies', company_list, name='company-list')

    app.router.add_post('/{company}/contractors/set', contractor_set, name='contractor-set')
    app.router.add_get('/{company}/contractors', contractor_list, name='contractor-list')
    app.router.add_get('/{company}/contractors/{id:\d+}', contractor_get, name='contractor-get')


def create_app(loop, *, settings=None):
    app = web.Application(loop=loop, middlewares=middleware)
    settings = settings or load_settings()
    app.update(settings, settings=settings)

    ctx = dict(
        commit=os.getenv('COMMIT', '-'),
        release_date=os.getenv('RELEASE_DATE', '-'),
    )
    index_html = (THIS_DIR / 'index.html').read_text()
    for key, value in ctx.items():
        index_html = re.sub('\{\{ ?%s ?\}\}' % key, value, index_html)
    app['index_html'] = index_html
    app.on_startup.append(startup)
    app.on_cleanup.append(cleanup)

    setup_routes(app)
    return app
