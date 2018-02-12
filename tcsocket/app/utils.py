import datetime as datetime
import json
from decimal import Decimal
from types import GeneratorType
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


class HTTPTooManyRequestsJson(HTTPClientErrorJson):
    status_code = 429


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
