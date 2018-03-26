import datetime as datetime
import json
import re
from decimal import Decimal
from types import GeneratorType
from typing import Any, Callable
from uuid import UUID

from aiohttp import web
from aiohttp.web_response import Response
from aiopg.sa.result import RowProxy


def isoformat(o):
    return o.isoformat()


class UniversalEncoder(json.JSONEncoder):
    ENCODER_BY_TYPE = {
        UUID: str,
        datetime.datetime: isoformat,
        datetime.date: isoformat,
        datetime.time: isoformat,
        set: list,
        frozenset: list,
        GeneratorType: list,
        bytes: lambda o: o.decode(),
        Decimal: str,
        RowProxy: dict,
    }

    def default(self, obj):
        try:
            encoder = self.ENCODER_BY_TYPE[type(obj)]
        except KeyError:
            return super().default(obj)
        return encoder(obj)


def pretty_lenient_json(data):
    return json.dumps(data, indent=2, sort_keys=True, cls=UniversalEncoder) + '\n'


JSON_CONTENT_TYPE = 'application/json'
ACCESS_CONTROL_HEADERS = {'Access-Control-Allow-Origin': '*'}


class HTTPClientErrorJson(web.HTTPClientError):
    def __init__(self, **data):
        super().__init__(
            text=pretty_lenient_json(data),
            content_type=JSON_CONTENT_TYPE,
            headers=ACCESS_CONTROL_HEADERS,
        )


class HTTPBadRequestJson(HTTPClientErrorJson):
    status_code = 400


class HTTPUnauthorizedJson(HTTPClientErrorJson):
    status_code = 401


class HTTPForbiddenJson(HTTPClientErrorJson):
    status_code = 403


class HTTPNotFoundJson(HTTPClientErrorJson):
    status_code = 404


class HTTPConflictJson(HTTPClientErrorJson):
    status_code = 409


def pretty_json(data):
    return json.dumps(data, indent=2) + '\n'


def json_response(request, *, status_=200, list_=None, **data):
    if JSON_CONTENT_TYPE in request.headers.get('Accept', ''):
        to_json = json.dumps
    else:
        to_json = pretty_json

    return Response(
        body=to_json(data if list_ is None else list_).encode(),
        status=status_,
        content_type=JSON_CONTENT_TYPE,
        headers=ACCESS_CONTROL_HEADERS,
    )


def slugify(name):
    name = (name or '').replace(' ', '-').lower()
    return re.sub('[^a-z0-9\-]', '', name)


def route_url(request, view_name, **kwargs):
    return str(request.app.router[view_name].url_for(**{k: str(v) for k, v in kwargs.items()}))


def get_arg(request, field, *, decoder: Callable[[str], Any]=int, default: Any=None):
    v = request.query.get(field, default)
    try:
        return None if v is None else decoder(v)
    except ValueError:
        raise HTTPBadRequestJson(
            status='invalid_argument',
            details=f'"{field}" had an invalid value "{v}"',
        )


def get_pagination(request, pag_default=30, pag_max=50):
    page = get_arg(request, 'page', default=1)
    pagination = min(get_arg(request, 'pagination', default=pag_default), pag_max)
    offset = (page - 1) * pagination
    return pagination, offset
