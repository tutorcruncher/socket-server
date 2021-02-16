import os
import re
from html import escape

from aiohttp import ClientSession, web
from aiopg.sa import create_engine
from arq import create_pool

from .middleware import middleware
from .settings import THIS_DIR, Settings
from .views import favicon, index, labels_list, qual_level_list, robots_txt, subject_list
from .views.appointments import (
    appointment_list,
    appointment_webhook,
    appointment_webhook_delete,
    book_appointment,
    check_client,
    service_list, appointment_webhook_clear,
)
from .views.company import company_create, company_list, company_options, company_update
from .views.contractor import contractor_get, contractor_list, contractor_set
from .views.enquiry import clear_enquiry, enquiry


async def startup(app: web.Application):
    settings: Settings = app['settings']
    redis = await create_pool(settings.redis_settings)
    app.update(
        pg_engine=await create_engine(settings.pg_dsn), redis=redis, session=ClientSession(),
    )


async def cleanup(app: web.Application):
    app['pg_engine'].close()
    await app['pg_engine'].wait_closed()
    app['redis'].close()
    await app['redis'].wait_closed()
    await app['session'].close()


def setup_routes(app):
    app.router.add_get(r'/', index, name='index')
    app.router.add_get(r'/robots.txt', robots_txt, name='robots-txt')
    app.router.add_get(r'/favicon.ico', favicon, name='favicon')
    app.router.add_post(r'/companies/create', company_create, name='company-create')
    app.router.add_get(r'/companies', company_list, name='company-list')

    app.router.add_get(r'/{company}/options', company_options, name='company-options')

    # to work with tutorcruncher websockets
    app.router.add_post(r'/{company}/webhook/options', company_update, name='company-update')
    app.router.add_post(r'/{company}/webhook/contractor', contractor_set, name='webhook-contractor')
    app.router.add_post(r'/{company}/webhook/clear-enquiry', clear_enquiry, name='webhook-clear-enquiry')
    app.router.add_post(r'/{company}/webhook/appointments/{id:\d+}', appointment_webhook, name='webhook-appointment')
    app.router.add_delete(
        r'/{company}/webhook/appointments/{id:\d+}', appointment_webhook_delete, name='webhook-appointment-delete'
    )
    app.router.add_delete(r'/{company}/webhook/appointments/clear', appointment_webhook_clear,
                          name='webhook-appointment-clear'
                          )

    app.router.add_get(r'/{company}/contractors', contractor_list, name='contractor-list')
    app.router.add_get(r'/{company}/contractors/{id:\d+}', contractor_get, name='contractor-get')
    app.router.add_route(r'*', '/{company}/enquiry', enquiry, name='enquiry')
    app.router.add_get(r'/{company}/subjects', subject_list, name='subject-list')
    app.router.add_get(r'/{company}/qual-levels', qual_level_list, name='qual-level-list')
    app.router.add_get(r'/{company}/labels', labels_list, name='labels')

    app.router.add_get(r'/{company}/appointments', appointment_list, name='appointment-list')
    app.router.add_get(r'/{company}/services', service_list, name='service-list')
    app.router.add_get(r'/{company}/check-client', check_client, name='check-client')
    app.router.add_post(r'/{company}/book-appointment', book_appointment, name='book-appointment')


def create_app(loop, *, settings: Settings = None):
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
