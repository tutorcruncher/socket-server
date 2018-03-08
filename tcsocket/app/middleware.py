import hashlib
import hmac
import logging
from asyncio import CancelledError
from time import time

from aiohttp.hdrs import METH_GET, METH_HEAD
from aiohttp.web_exceptions import HTTPBadRequest, HTTPException, HTTPInternalServerError, HTTPMovedPermanently
from aiohttp.web_middlewares import middleware
from aiohttp.web_urldispatcher import SystemRoute
from pydantic import ValidationError
from sqlalchemy import select
from yarl import URL

from .models import sa_companies
from .utils import HTTPBadRequestJson, HTTPForbiddenJson, HTTPNotFoundJson, HTTPUnauthorizedJson
from .validation import VIEW_MODELS

request_logger = logging.getLogger('socket.request')

PUBLIC_VIEWS = {
    'index',
    'robots-txt',
    'favicon',
    'contractor-list',
    'company-options',
    'contractor-get',
    'enquiry',
    'subject-list',
    'qual-level-list',
    'labels',
    'appointment-list',
    'service-list',
    'check-client',
}


async def log_extra(request, response=None):
    return {'data': dict(
        request_url=str(request.rel_url),
        request_method=request.method,
        request_host=request.host,
        request_headers=dict(request.headers),
        request_text=await request.text(),
        response_status=getattr(response, 'status', None),
        response_headers=dict(getattr(response, 'headers', {})),
        response_text=getattr(response, 'text', None)
    )}


async def log_warning(request, response):
    request_logger.warning('%s %d', request.rel_url, response.status, extra={
        'fingerprint': [request.rel_url, str(response.status)],
        'data': await log_extra(request, response)
    })


@middleware
async def error_middleware(request, handler):
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


@middleware
async def pg_conn_middleware(request, handler):
    async with ConnectionManager(request.app['pg_engine']) as conn_manager:
        request['conn_manager'] = conn_manager
        return await handler(request)


def domain_allowed(allow_domains, current_domain):
    return (
        current_domain and
        (
            current_domain.endswith('tutorcruncher.com') or
            any(
                allow_domain == current_domain or
                (allow_domain.startswith('*') and current_domain.endswith(allow_domain[1:]))
                for allow_domain in allow_domains
            )
        )
    )


@middleware
async def company_middleware(request, handler):
    try:
        public_key = request.match_info.get('company')
        if public_key:
            c = sa_companies.c
            select_fields = c.id, c.name, c.public_key, c.private_key, c.name_display, c.options, c.domains
            q = select(select_fields).where(c.public_key == public_key)
            conn = await request['conn_manager'].get_connection()
            result = await conn.execute(q)
            company = await result.first()

            if company and company.domains is not None:
                origin = request.headers.get('Origin') or request.headers.get('Referer')
                if origin and not domain_allowed(company.domains, URL(origin).host):
                    raise HTTPForbiddenJson(
                        status='wrong Origin domain',
                        details=f"the current Origin '{origin}' does not match the allowed domains",
                    )
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


@middleware
async def json_request_middleware(request, handler):
    if request.method not in {METH_GET, METH_HEAD} and request.match_info.route.name:
        error_details = None
        try:
            data = await request.json()
        except ValueError as e:
            error_details = f'Value Error: {e}'
        else:
            request['body_request_time'] = data.pop('_request_time', None)
            model = VIEW_MODELS.get(request.match_info.route.name)
            if model:
                try:
                    request['model'] = model.parse_obj(data)
                except ValidationError as e:
                    error_details = e.errors_dict

        if error_details:
            raise HTTPBadRequestJson(
                status='invalid request data',
                details=error_details,
            )
    return await handler(request)


def _check_timestamp(ts: str, now):
    try:
        offset = now - int(ts)
        if not 10 > offset > -1:
            raise ValueError()
    except (TypeError, ValueError):
        raise HTTPForbiddenJson(
            status='invalid request time',
            details=f"request time '{ts}' not in the last 10 seconds",
        )


async def authenticate(request, api_key=None):
    api_key_choices = api_key, request.app['settings'].master_key
    now = time()
    if request.method == METH_GET:
        r_time = request.headers.get('Request-Time', '<missing>')
        _check_timestamp(r_time, now)
        body = r_time.encode()
    else:
        _check_timestamp(request['body_request_time'], now)
        body = await request.read()
    signature = request.headers.get('Signature', request.headers.get('Webhook-Signature', '<missing>'))
    for _api_key in api_key_choices:
        if _api_key and signature == hmac.new(_api_key, body, hashlib.sha256).hexdigest():
            return
    raise HTTPUnauthorizedJson(
        status='invalid signature',
        details=f'Signature header "{signature}" does not match computed signature',
    )


@middleware
async def auth_middleware(request, handler):
    if isinstance(request.match_info.route, SystemRoute):
        # eg. 404
        return await handler(request)
    route_name = request.match_info.route.name
    route_name = route_name and route_name.replace('-head', '')
    if route_name not in PUBLIC_VIEWS:
        company = request.get('company')
        if company:
            await authenticate(request, company.private_key.encode())
        else:
            await authenticate(request)
    return await handler(request)


middleware = (
    error_middleware,
    pg_conn_middleware,
    company_middleware,
    json_request_middleware,
    auth_middleware,
)
