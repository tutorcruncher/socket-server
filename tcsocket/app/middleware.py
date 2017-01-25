import hashlib
import hmac
from asyncio import CancelledError
from datetime import datetime, timedelta

import trafaret as t
from aiohttp.hdrs import METH_GET, METH_POST
from aiohttp.web_exceptions import HTTPBadRequest
from sqlalchemy import select

from .models import sa_companies
from .utils import HTTPBadRequestJson, HTTPForbiddenJson, HTTPNotFoundJson, HTTPUnauthorizedJson
from .views import VIEW_SCHEMAS

PUBLIC_VIEWS = {
    'index',
    'contractor-list',
    'contractor-get',
}


async def json_request_middleware(app, handler):
    async def _handler(request):
        if request.method == METH_POST and request.match_info.route.name:
            error_details = None
            schema = VIEW_SCHEMAS[request.match_info.route.name]
            try:
                data = await request.json()
                request['json_obj'] = schema.check(data)
            except t.DataError as e:
                error_details = e.as_dict()
            except ValueError as e:
                error_details = f'Value Error: {e}'

            if error_details:
                raise HTTPBadRequestJson(
                    status='invalid request data',
                    details=error_details,
                )
        return await handler(request)
    return _handler


class ConnectionManager:
    """
    Copies engine.acquire()'s context manager but is lazy in that you need to call get_connection()
    for a connection to be found, otherwise does nothing.
    """
    def __init__(self, engine):
        self._engine = engine
        self._conn = None
        self._entered = False

    async def __aenter__(self):
        self._entered = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if self._conn is not None:
                await self._engine.release(self._conn)
                self._conn = None
        except CancelledError:
            raise HTTPBadRequest()

    async def get_connection(self):
        assert self._entered
        if self._conn is None:
            self._conn = await self._engine._acquire()
        return self._conn


async def pg_conn_middleware(app, handler):
    async def _handler(request):
        async with ConnectionManager(app['pg_engine']) as conn_manager:
            request['conn_manager'] = conn_manager
            return await handler(request)
    return _handler


async def company_middleware(app, handler):
    async def _handler(request):
        # if hasattr(request.match_info.route, 'status'):
        try:
            public_key = request.match_info.get('company')
            if public_key:
                c = sa_companies.c
                select_fields = c.id, c.public_key, c.private_key, c.name_display
                q = select(select_fields).where(c.public_key == public_key)
                conn = await request['conn_manager'].get_connection()
                result = await conn.execute(q)
                company = await result.first()
                if company:
                    request['company'] = company
                else:
                    raise HTTPNotFoundJson(
                        status='company not found',
                        details=f'No company found for key {public_key}',
                    )
            return await handler(request)
        except CancelledError:
            raise HTTPBadRequest()
    return _handler


async def authenticate(request, api_key=None):
    api_key_choices = api_key, request.app['master_key']
    if request.method == METH_GET:
        r_time = request.headers.get('Request-Time', '<missing>')
        now = datetime.now()
        try:
            assert (now - timedelta(seconds=10)) < datetime.fromtimestamp(int(r_time)) < now
        except (ValueError, AssertionError):
            raise HTTPForbiddenJson(
                status='invalid request time',
                details=f'Request-Time header "{r_time}" not in the last 10 seconds',
            )
        else:
            body = r_time.encode()
    else:
        body = await request.read()
    signature = request.headers.get('Signature', request.headers.get('Webhook-Signature', '<missing>'))
    for _api_key in api_key_choices:
        if _api_key and signature == hmac.new(_api_key, body, hashlib.sha256).hexdigest():
            return
    raise HTTPUnauthorizedJson(
        status='invalid signature',
        details=f'Signature header "{signature}" does not match computed signature',
    )


async def auth_middleware(app, handler):
    async def _handler(request):
        # status check avoids messing with requests which have already been processed, eg. 404
        if not hasattr(request.match_info.route, 'status') and request.match_info.route.name not in PUBLIC_VIEWS:
            company = request.get('company')
            if company:
                await authenticate(request, company.private_key.encode())
            else:
                await authenticate(request)
        return await handler(request)
    return _handler

middleware = pg_conn_middleware, company_middleware, json_request_middleware, auth_middleware
