import datetime as datetime
import json
from decimal import Decimal
from types import GeneratorType
from uuid import UUID

from aiohttp import web
from aiohttp.web_reqrep import Response
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


def to_pretty_json(data):
    return json.dumps(data, indent=2, sort_keys=True, cls=UniversalEncoder) + '\n'


JSON_CONTENT_TYPE = 'application/json'
# we could change this to enforce the right site, but it wouldn't add much security and would confuse people
PUBLIC_HEADERS = {
    'Access-Control-Allow-Origin': '*',
}


class HTTPClientErrorJson(web.HTTPClientError):
    def __init__(self, **data):
        super().__init__(body=to_pretty_json(data).encode(), content_type=JSON_CONTENT_TYPE)


class HTTPUnauthorizedJson(HTTPClientErrorJson):
    status_code = 401


class HTTPBadRequestJson(HTTPClientErrorJson):
    status_code = 400


class HTTPForbiddenJson(HTTPClientErrorJson):
    status_code = 403


class HTTPNotFoundJson(HTTPClientErrorJson):
    status_code = 404


def pretty_json_response(*, status_=200, list_=None, **data):
    return Response(text=to_pretty_json(list_ or data), status=status_, content_type=JSON_CONTENT_TYPE)


def public_json_response(*, status_=200, list_=None, **data):
    return Response(
        text=json.dumps(list_ or data),
        status=status_,
        content_type=JSON_CONTENT_TYPE,
        headers=PUBLIC_HEADERS,
    )
