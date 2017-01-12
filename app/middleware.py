import hashlib
import hmac

from aiohttp.hdrs import METH_POST
import trafaret as t

from .utils import HTTPForbiddenPrettyJson

PUBLIC_VIEWS = {
    'index',
}


async def auth_middleware(app, handler):
    async def _handler(request):
        if request.match_info.route.name not in PUBLIC_VIEWS:
            request['body'] = await request.read()
            m = hmac.new(request.app['shared_secret'], request['body'], hashlib.sha256)
            signature = request.headers.get('Webhook-Signature', '-')
            if signature != m.hexdigest():
                raise HTTPForbiddenPrettyJson(
                    status='invalid signature',
                    details=f"Webhook-Signature header '{signature}' does not match computed signature",
                )
        return await handler(request)
    return _handler


VIEW_SCHEMAS = {
    'create-company': t.Dict({
        'name': t.String(min_length=4, max_length=63),
        t.Key('site_domain', optional=True): t.String(min_length=4, max_length=63),
        t.Key('name_display', optional=True): t.Or(
            t.Atom('first_name') |
            t.Atom('first_name_initial') |
            t.Atom('full_name')
        ),
    })
}


async def json_middleware(app, handler):
    async def _handler(request):
        if request.method == METH_POST:
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
                raise HTTPForbiddenPrettyJson(
                    status='invalid request',
                    details=error_details,
                )
        return await handler(request)
    return _handler
