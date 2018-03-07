import os
import re
from html import escape

from aiohttp import ClientSession, web
from aiopg.sa import create_engine
from arq import create_pool_lenient

from .middleware import middleware
from .settings import THIS_DIR, Settings
from .views import favicon, index, labels_list, qual_level_list, robots_txt, subject_list
from .views.appointments import appointment_list, appointment_webhook, appointment_webhook_delete, service_list
from .views.company import company_create, company_list, company_options, company_update
from .views.contractor import contractor_get, contractor_list, contractor_set
from .views.enquiry import clear_enquiry, enquiry
from .worker import MainActor


async def startup(app: web.Application):
    settings: Settings = app['settings']
    redis = await create_pool_lenient(settings.redis_settings, app.loop)
    app.update(
        pg_engine=await create_engine(settings.pg_dsn, loop=app.loop),
        redis=redis,
        worker=MainActor(settings=settings, existing_redis=redis),
        session=ClientSession(loop=app.loop),
    )
    await app['worker'].startup()


async def cleanup(app: web.Application):
    app['pg_engine'].close()
    await app['pg_engine'].wait_closed()
    await app['worker'].close(True)
    await app['session'].close()


def setup_routes(app):
    app.router.add_get('/', index, name='index')
    app.router.add_get('/robots.txt', robots_txt, name='robots-txt')
    app.router.add_get('/favicon.ico', favicon, name='favicon')
    app.router.add_post('/companies/create', company_create, name='company-create')
    app.router.add_get('/companies', company_list, name='company-list')

    app.router.add_get('/{company}/options', company_options, name='company-options')

    # to work with tutorcruncher websockets
    app.router.add_post('/{company}/webhook/options', company_update, name='company-update')
    app.router.add_post('/{company}/webhook/contractor', contractor_set, name='webhook-contractor')
    app.router.add_post('/{company}/webhook/clear-enquiry', clear_enquiry, name='webhook-clear-enquiry')
    app.router.add_post('/{company}/webhook/appointments/{id:\d+}', appointment_webhook, name='webhook-appointment')
    app.router.add_delete('/{company}/webhook/appointments/{id:\d+}', appointment_webhook_delete,
                          name='webhook-appointment-delete')

    app.router.add_get('/{company}/contractors', contractor_list, name='contractor-list')
    app.router.add_get('/{company}/contractors/{id:\d+}', contractor_get, name='contractor-get')
    app.router.add_route('*', '/{company}/enquiry', enquiry, name='enquiry')
    app.router.add_get('/{company}/subjects', subject_list, name='subject-list')
    app.router.add_get('/{company}/qual-levels', qual_level_list, name='qual-level-list')
    app.router.add_get('/{company}/labels', labels_list, name='labels')

    app.router.add_get('/{company}/appointments', appointment_list, name='appointment-list')
    app.router.add_get('/{company}/services', service_list, name='service-list')


def create_app(loop, *, settings: Settings=None):
    app = web.Application(middlewares=middleware)
    settings = settings or Settings()
    app['settings'] = settings

    ctx = dict(
        COMMIT=os.getenv('COMMIT', '-'),
        RELEASE_DATE=os.getenv('RELEASE_DATE', '-'),
        SERVER_NAME=os.getenv('SERVER_NAME', '-'),
    )
    index_html = (THIS_DIR / 'index.html').read_text()
    for key, value in ctx.items():
        index_html = re.sub(r'\{\{ ?%s ?\}\}' % key, escape(value), index_html)
    app['index_html'] = index_html
    app.on_startup.append(startup)
    app.on_cleanup.append(cleanup)

    setup_routes(app)
    return app
