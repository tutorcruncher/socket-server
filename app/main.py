from aiohttp import web
from aiopg.sa import create_engine
from sqlalchemy.engine.url import URL

from .middleware import middleware
from .settings import load_settings
from .views import company_create, contractor_get, contractor_list, contractor_set, index
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
    app.router.add_get('/{company}/contractors', contractor_list, name='contractor-list')
    app.router.add_get('/{company}/contractors/{id:\d+}-{slug}', contractor_get, name='contractor-get')
    app.router.add_post('/{company}/contractors/set', contractor_set, name='contractor-set')


def create_app(loop, *, settings=None):
    app = web.Application(loop=loop, middlewares=middleware)
    app['name'] = 'socket-server'
    settings = settings or load_settings()
    app.update(settings, settings=settings)

    app.on_startup.append(startup)
    app.on_cleanup.append(cleanup)

    setup_routes(app)
    return app
