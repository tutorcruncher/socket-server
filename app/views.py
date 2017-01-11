from aiohttp.hdrs import METH_POST
from aiohttp.web_exceptions import HTTPFound
from aiohttp.web_reqrep import json_response
from aiohttp_jinja2 import template

from aiohttp_session import get_session

# from .models import sa_messages


@template('index.jinja')
async def index(request):
    """
    This is the view handler for the "/" url.

    :param request: the request object see http://aiohttp.readthedocs.io/en/stable/web_reference.html#request
    :return: context for the template.
    """
    # Note: we return a dict not a response because of the @template decorator
    return {
        'title': request.app['name'],
        'intro': "Success! you've setup a basic aiohttp app.",
    }


async def process_form(request):
    new_message, missing_fields = {}, []
    fields = ['username', 'message']
    data = await request.post()
    for f in fields:
        new_message[f] = data.get(f)
        if not new_message[f]:
            missing_fields.append(f)

    if missing_fields:
        return 'Invalid form submission, missing fields: {}'.format(', '.join(missing_fields))

    # simple demonstration of sessions by saving the username and pre-populating it in the form next time
    session = await get_session(request)
    session['username'] = new_message['username']

    async with request.app['pg_engine'].acquire() as conn:
        await conn.execute(sa_messages.insert().values(
            username=new_message['username'],
            message=new_message['message'],
        ))
    raise HTTPFound(request.app.router['messages'].url())


@template('messages.jinja')
async def messages(request):
    if request.method == METH_POST:
        # the 302 redirect is processed as an exception, so if this coroutine returns there's a form error
        form_errors = await process_form(request)
    else:
        form_errors = None

    # simple demonstration of sessions by pre-populating username if it's already been set
    session = await get_session(request)
    username = session.get('username', '')

    return {
        'title': 'Message board',
        'form_errors': form_errors,
        'username': username,
    }


async def message_data(request):
    """
    As an example of aiohttp providing a non-html response, we load the actual messages for the "messages" view above
    via ajax using this endpoint to get data. see static/message_display.js for details of rendering.
    """
    messages = []

    async with request.app['pg_engine'].acquire() as conn:
        async for row in conn.execute(sa_messages.select().order_by(sa_messages.c.timestamp.desc())):
            ts = '{:%Y-%m-%d %H:%M:%S}'.format(row.timestamp)
            messages.append({'username': row.username, 'timestamp':  ts, 'message': row.message})
    return json_response(messages)
