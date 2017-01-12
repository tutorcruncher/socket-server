from secrets import token_hex

from aiohttp.web_reqrep import json_response
from psycopg2._psycopg import IntegrityError

from .models import sa_companies
from .utils import HTTPForbiddenPrettyJson, pretty_json_response


async def index(request):
    return json_response({
        'title': 'TODO',
    })


async def company_create(request):
    """
    Create a new company.

    Authentication and json parsing are done by middleware.
    """
    data = request['json_obj']
    data['key'] = token_hex(10)
    async with request.app['pg_engine'].acquire() as conn:
        return_cols = sa_companies.c.key, sa_companies.c.name, sa_companies.c.site_domain, sa_companies.c.name_display
        try:
            v = await conn.execute(sa_companies.insert().values(**data).returning(*return_cols))
        except IntegrityError as e:
            raise HTTPForbiddenPrettyJson(
                status='data integrity error',
                details=f'Integrity Error: {e}',
            )
        else:
            new_data = await v.first()
            return pretty_json_response({
                'status': 'success',
                'details': dict(new_data)
            }, status=201)


async def contractor_set(request):
    """
    Create a new company.

    Authentication and json parsing are done by middleware.
    """
    data = request['json_obj']
    data['key'] = token_hex(10)
    async with request.app['pg_engine'].acquire() as conn:
        return_cols = sa_companies.c.key, sa_companies.c.name, sa_companies.c.site_domain, sa_companies.c.name_display
        try:
            v = await conn.execute(sa_companies.insert().values(**data).returning(*return_cols))
        except IntegrityError as e:
            raise HTTPForbiddenPrettyJson(
                status='data integrity error',
                details=f'Integrity Error: {e}',
            )
        else:
            new_data = await v.first()
            return pretty_json_response({
                'status': 'success',
                'details': dict(new_data)
            }, status=201)


async def contractor_list(request):
    return json_response({})

async def contractor_get(request):
    return json_response({})
