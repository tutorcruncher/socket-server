import os
import re
from html import escape

from aiohttp import web
from aiopg.sa import create_engine

from .middleware import middleware
from .settings import THIS_DIR, load_settings, pg_dsn
from .views import company_create, company_list, contractor_get, contractor_list, contractor_set, enquiry, index
from .worker import MainActor


async def startup(app: web.Application):
    app.update(
        pg_engine=await create_engine(pg_dsn(app['database']), loop=app.loop),
        worker=MainActor(settings=app['settings']),
    )
    await app['worker'].startup()


async def cleanup(app: web.Application):
    app['pg_engine'].close()
    await app['pg_engine'].wait_closed()
    await app['worker'].close(True)


def setup_routes(app):
    app.router.add_get('/', index, name='index')
    app.router.add_post('/companies/create', company_create, name='company-create')
    app.router.add_get('/companies', company_list, name='company-list')

    app.router.add_post('/{company}/contractors/set', contractor_set, name='contractor-set')
    app.router.add_get('/{company}/contractors', contractor_list, name='contractor-list')
    app.router.add_get('/{company}/contractors/{id:\d+}', contractor_get, name='contractor-get')
    app.router.add_route('*', '/{company}/enquiry', enquiry, name='enquiry')


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
        index_html = re.sub(r'\{\{ ?%s ?\}\}' % key, escape(value), index_html)
    app['index_html'] = index_html
    app.on_startup.append(startup)
    app.on_cleanup.append(cleanup)

    setup_routes(app)
    return app
