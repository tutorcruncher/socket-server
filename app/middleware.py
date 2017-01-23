import hashlib
import hmac

import trafaret as t
from aiohttp.hdrs import METH_POST
from sqlalchemy import select

from app.models import sa_companies

from .utils import HTTPBadRequestJson, HTTPNotFoundJson, HTTPUnauthorizedJson
from .views import VIEW_SCHEMAS

PUBLIC_VIEWS = {
    'index',
    'contractor-list',
    'contractor-get',
}


async def auth_middleware(app, handler):
    async def _handler(request):
        if not hasattr(request.match_info.route, 'status') and request.match_info.route.name not in PUBLIC_VIEWS:
            body = await request.read()
            m = hmac.new(request.app['shared_secret'], body, hashlib.sha256)
            signature = request.headers.get('Webhook-Signature', '<missing>')
            if signature != m.hexdigest():
                raise HTTPUnauthorizedJson(
                    status='invalid signature',
                    details=f'Webhook-Signature header "{signature}" does not match computed signature',
                )
        return await handler(request)
    return _handler


async def json_request_middleware(app, handler):
    async def _handler(request):
        view_name = request.match_info.route.name
        if request.method == METH_POST and view_name:
            error_details = None
            try:
                schema = VIEW_SCHEMAS[view_name]
            except KeyError as e:
                raise KeyError(f'can\'t find "{view_name}" in {sorted(VIEW_SCHEMAS.keys())}') from e

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

    async def __aenter__(self):
        self._conn = None
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._conn is not None:
            await self._engine.release(self._conn)
            self._conn = None

    async def get_connection(self):
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
        company_key = request.match_info.get('company')
        if company_key:
            select_fields = sa_companies.c.id, sa_companies.c.key, sa_companies.c.name_display
            q = select(select_fields).where(sa_companies.c.key == company_key)
            conn = await request['conn_manager'].get_connection()
            result = await conn.execute(q)
            company = await result.first()
            if company:
                request['company'] = company
            else:
                raise HTTPNotFoundJson(
                    status='company not found',
                    details=f'No company found for key {company_key}',
                )
        return await handler(request)
    return _handler

middleware = auth_middleware, json_request_middleware, pg_conn_middleware, company_middleware
