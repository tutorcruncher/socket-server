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
        bytes: lambda o: o.decode('utf8'),
        Decimal: str,
        RowProxy: dict,
    }

    def default(self, obj):
        try:
            encoder = self.ENCODER_BY_TYPE[type(obj)]
        except KeyError:
            try:
                return json.JSONEncoder.default(self, obj)
            except TypeError:
                return '%s: %r' % (obj.__class__.__name__, obj)
        return encoder(obj)


def to_pretty_json(data):
    return json.dumps(data, indent=2, sort_keys=True, cls=UniversalEncoder) + '\n'


JSON_CONTENT_TYPE = 'application/json'


class HTTPClientErrorJson(web.HTTPClientError):
    def __init__(self, **data):
        super().__init__(body=to_pretty_json(data).encode(), content_type=JSON_CONTENT_TYPE)


class HTTPBadRequestJson(HTTPClientErrorJson):
    status_code = 400


class HTTPForbiddenJson(HTTPClientErrorJson):
    status_code = 403


class HTTPNotFoundJson(HTTPClientErrorJson):
    status_code = 404


def json_response(data, *, request, status=200):
    if request.app['debug']:
        return Response(text=to_pretty_json(data), status=status, content_type=JSON_CONTENT_TYPE)
    else:
        return Response(text=json.dumps(data), status=status, content_type=JSON_CONTENT_TYPE)
