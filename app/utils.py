import json

from aiohttp import web
from aiohttp.web_reqrep import Response


class HTTPForbiddenPrettyJson(web.HTTPForbidden):
    def __init__(self, **data):
        text = json.dumps(data, indent=2, sort_keys=True) + '\n'
        super().__init__(body=text.encode(), content_type='application/json')


def pretty_json_response(data, status=200):
    text = json.dumps(data, indent=2, sort_keys=True) + '\n'
    return Response(text=text, status=status, content_type='application/json')
