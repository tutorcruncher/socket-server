import hashlib
import hmac
import logging
from asyncio import CancelledError
from datetime import datetime, timedelta

from aiohttp.hdrs import METH_GET, METH_POST
from aiohttp.web_exceptions import HTTPBadRequest, HTTPException, HTTPInternalServerError, HTTPMovedPermanently
from pydantic import ValidationError
from sqlalchemy import select

from .models import sa_companies
from .utils import HTTPBadRequestJson, HTTPForbiddenJson, HTTPNotFoundJson, HTTPUnauthorizedJson
from .validation import VIEW_MODELS

request_logger = logging.getLogger('socket.request')

PUBLIC_VIEWS = {
    'index',
    'robots-txt',
    'favicon',
    'contractor-list',
    'contractor-get',
    'enquiry',
    'subject-list',
    'qual-level-list',
}


async def log_extra(request, response=None):
    return {'data': dict(
        request_url=str(request.rel_url),
        request_method=request.method,
        request_host=request.host,
        request_headers=dict(request.headers),
        request_text=response and await request.text(),
        response_status=response and response.status,
        response_headers=response and dict(response.headers),
        response_text=response and response.text,
    )}


async def log_warning(request, response):
    request_logger.warning('%s %d', request.rel_url, response.status, extra={
        'fingerprint': [request.rel_url, str(response.status)],
        'data': await log_extra(request, response)
    })


async def error_middleware(app, handler):
    async def _handler(request):
        try:
            http_exception = getattr(request.match_info, 'http_exception', None)
            if http_exception:
                raise http_exception
            else:
                r = await handler(request)
        except HTTPException as e:
            if request.method == METH_GET and e.status == 404 and request.rel_url.raw_path.endswith('/'):
                possible_path = request.rel_url.raw_path[:-1]
                for resource in request.app.router._resources:
                    match_dict = resource._match(possible_path)
                    if match_dict:
                        raise HTTPMovedPermanently(possible_path)
            if e.status > 310:
                await log_warning(request, e)
            raise
        except BaseException as e:
            request_logger.exception('%s: %s', e.__class__.__name__, e, extra={
                'fingerprint': [e.__class__.__name__, str(e)],
                'data': await log_extra(request)
            })
            raise HTTPInternalServerError()
        else:
            if r.status > 310:
                await log_warning(request, r)
        return r
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
                select_fields = c.id, c.public_key, c.private_key, c.name_display, c.domain
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


async def json_request_middleware(app, handler):
    async def _handler(request):
        if request.method == METH_POST and request.match_info.route.name:
            error_details = None
            model = VIEW_MODELS[request.match_info.route.name]
            try:
                data = await request.json()
                request['json_obj'] = model.parse_obj(data).dict()
            except ValidationError as e:
                error_details = e.errors_dict
            except ValueError as e:
                error_details = f'Value Error: {e}'

            if error_details:
                raise HTTPBadRequestJson(
                    status='invalid request data',
                    details=error_details,
                )
        return await handler(request)
    return _handler


async def authenticate(request, api_key=None):
    api_key_choices = api_key, request.app['settings'].master_key
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
        route_name = request.match_info.route.name
        route_name = route_name and route_name.replace('-head', '')
        if route_name not in PUBLIC_VIEWS:
            company = request.get('company')
            if company:
                await authenticate(request, company.private_key.encode())
            else:
                await authenticate(request)
        return await handler(request)
    return _handler

middleware = (
    error_middleware,
    pg_conn_middleware,
    company_middleware,
    json_request_middleware,
    auth_middleware,
)
