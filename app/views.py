from datetime import datetime
from secrets import token_hex

import re
import trafaret as t
from psycopg2._psycopg import IntegrityError
from dateutil.parser import parse as dt_parse
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .models import sa_companies, sa_contractors, Action, NameOptions
from .utils import HTTPBadRequestJson, json_response

ANY_DICT = t.Dict()
ANY_DICT.allow_extra('*')


VIEW_SCHEMAS = {
    'company-create': t.Dict({
        'name': t.String(min_length=4, max_length=63),
        t.Key('site_domain', optional=True): t.String(min_length=4, max_length=63),
        t.Key('name_display', optional=True): t.Or(
            t.Atom('first_name') |
            t.Atom('first_name_initial') |
            t.Atom('full_name')
        ),
    }),
    'contractor-set': t.Dict({
        'id': t.Int(),
        t.Key('first_name', optional=True): t.String(max_length=63),
        t.Key('last_name', optional=True): t.String(max_length=63),

        t.Key('town', optional=True): t.String(max_length=63),
        t.Key('country', optional=True): t.String(max_length=63),
        t.Key('location', optional=True): t.Dict({
            'latitude': t.Or(t.Float() | t.Null),
            'longitude': t.Or(t.Float() | t.Null),
        }),

        t.Key('extra_attributes', optional=True): t.List(ANY_DICT),

        t.Key('image', optional=True): t.String(max_length=63),
        t.Key('last_updated', optional=True): t.String() >> dt_parse,
        t.Key('photo', optional=True): t.URL(),
    })
}
VIEW_SCHEMAS['contractor-set'].ignore_extra('*')


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
    try:
        v = await request['conn'].execute((
            sa_companies
            .insert()
            .values(**data)
            .returning(sa_companies.c.id, sa_companies.c.key, sa_companies.c.name)
        ))
    except IntegrityError as e:
        # TODO we could format the error message better here
        raise HTTPBadRequestJson(
            status='data integrity error',
            details=f'Integrity Error: {e}',
        )
    new_data = await v.first()
    return json_response({
        'status': 'success',
        'details': new_data
    }, request=request, status=201)


async def contractor_set(request):
    """
    Create or update a contractor.
    """
    data = request['json_obj']
    location = data.pop('location', None)
    if location:
        data.update(location)
    photo = data.pop('photo', None)
    if photo:
        # TODO deal with photo
        pass
    data['last_updated'] = data.get('last_updated') or datetime.now()
    company_id = request['company'].id
    id = data.pop('id')
    # FIXME https://bitbucket.org/zzzeek/sqlalchemy/issues/3888
    update_data = data.copy()
    update_data.pop('extra_attributes')
    v = await request['conn'].execute(
        pg_insert(sa_contractors)
        .values(id=id, company=company_id, action=Action.insert, **data)
        .on_conflict_do_update(
            index_elements=[sa_contractors.c.id],
            where=sa_contractors.c.company == company_id,
            set_=dict(action=Action.update, **update_data)
        )
        .returning(sa_contractors.c.action)
    )
    status, desc = (201, 'created') if (await v.first()).action == Action.insert else (200, 'updated')
    return json_response({
        'status': 'success',
        'details': f'contractor {desc}',
    }, request=request, status=status)


SORT_OPTIONS = {
    'update': sa_contractors.c.last_updated.desc(),
    'name': sa_contractors.c.first_name.asc(),
    # TODO some configurable sort index
}
PAGINATION = 20


def _slugify(name):
    return re.sub('[^a-z]', '', name.replace(' ', '-').lower())


async def contractor_list(request):
    sort_on = SORT_OPTIONS.get(request.GET.get('sort'), SORT_OPTIONS['update'])
    page = request.GET.get('page', 1)
    try:
        page = int(page)
    except ValueError:
        raise HTTPBadRequestJson(
            status='invalid page',
            details=f'{page} is not a valid integer',
        )
    offset = (page - 1) * PAGINATION
    c = sa_contractors.c
    cols = [c.id, c.first_name, c.last_name, c.photo, c.tag_line]
    q = (
        select(cols)
        .order_by(sort_on)
        .offset(offset)
        .limit(PAGINATION)
    )
    results = []
    name_display = request['company'].name_display
    async for row in request['conn'].execute(q):
        name = row.first_name
        if name_display != NameOptions.first_name and row.last_name:
            if name_display == NameOptions.first_name_initial:
                name += ' ' + row.last_name[0]
            elif name_display == NameOptions.full_name:
                name += ' ' + row.last_name
        results.append(dict(
            id=row.id,
            slug=_slugify(name),
            name=name,
            tag_line=row.tag_line,
            photo=row.photo,  # TODO
        ))
    return json_response(results, request=request, status=200)

async def contractor_get(request):
    return json_response({}, request=request)
