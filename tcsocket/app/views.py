import json
import re
from itertools import groupby
from operator import attrgetter
from secrets import token_hex

import trafaret as t
from aiohttp.hdrs import METH_POST
from aiohttp.web import Response
from arq.utils import timestamp
from dateutil.parser import parse as dt_parse
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import and_

from .logs import logger
from .models import Action, NameOptions, sa_companies, sa_con_skills, sa_contractors, sa_qual_levels, sa_subjects
from .processing import contractor_set as _contractor_set
from .utils import HTTPBadRequestJson, pretty_json_response, public_json_response

EXTRA_ATTR_TYPES = 'checkbox', 'text_short', 'text_extended', 'integer', 'stars', 'dropdown', 'datetime', 'date'

AnyDict = t.Dict()
AnyDict.allow_extra('*')

VIEW_SCHEMAS = {
    'company-create': t.Dict({
        'name': t.String(min_length=4, max_length=63),
        t.Key('name_display', optional=True): t.Or(
            t.Atom('first_name') |
            t.Atom('first_name_initial') |
            t.Atom('full_name')
        ),
        t.Key('public_key', default=None): t.Or(t.Null | t.String(min_length=18, max_length=20)),
        t.Key('private_key', default=None): t.Or(t.Null | t.String(min_length=20, max_length=50)),
    }),
    'contractor-set': t.Dict({
        'id': t.Int(),
        t.Key('deleted', default=False): t.Bool,
        t.Key('first_name', optional=True): t.Or(t.Null | t.String(max_length=63)),
        t.Key('last_name', optional=True): t.Or(t.Null | t.String(max_length=63)),

        t.Key('town', optional=True): t.Or(t.Null | t.String(max_length=63)),
        t.Key('country', optional=True): t.Or(t.Null | t.String(max_length=63)),
        t.Key('location', optional=True): t.Or(t.Null | t.Dict({
            'latitude': t.Or(t.Float | t.Null),
            'longitude': t.Or(t.Float | t.Null),
        })),

        t.Key('extra_attributes', default=[]): t.List(t.Dict({
            'machine_name': t.Or(t.Null | t.String),
            'type': t.Or(*[t.Atom(eat) for eat in EXTRA_ATTR_TYPES]),
            'name': t.String,
            'value': t.Or(t.Bool | t.String | t.Float),
            'id': t.Int,
            'sort_index': t.Float,
        })),

        t.Key('skills', default=[]): t.List(t.Dict({
            'subject': t.String,
            t.Key('subject_id', optional=True): t.Int,  # not currently used
            'category': t.String,
            'qual_level': t.String,
            t.Key('qual_level_id', optional=True): t.Int,  # not currently used
            t.Key('qual_level_ranking', default=0): t.Float,
        })),

        t.Key('last_updated', optional=True): t.Or(t.Null | t.String >> dt_parse),
        t.Key('photo', optional=True): t.Or(t.Null | t.URL),
    }),
    'enquiry': t.Dict({
        'client_name': t.String(max_length=255),
        t.Key('client_email', optional=True): t.Or(t.Null | t.Email()),
        t.Key('client_phone', optional=True): t.Or(t.Null | t.String(max_length=255)),
        t.Key('service_recipient_name', optional=True): t.Or(t.Null | t.String(max_length=255)),
        t.Key('attributes', optional=True): t.Or(t.Null | AnyDict),

        t.Key('contractor', optional=True): t.Or(t.Null | t.Int(gt=0)),
        t.Key('subject', optional=True): t.Or(t.Null | t.Int(gt=0)),
        t.Key('qual_level', optional=True): t.Or(t.Null | t.Int(gt=0)),

        t.Key('http_referrer', optional=True): t.Or(t.Null | t.String(max_length=200)),
    })
}
VIEW_SCHEMAS['contractor-set'].ignore_extra('*')


async def index(request):
    return Response(text=request.app['index_html'], content_type='text/html')


async def company_create(request):
    """
    Create a new company.

    Authentication and json parsing are done by middleware.
    """
    data = request['json_obj']
    existing_company = bool(data['private_key'])
    data.update(
        public_key=data['public_key'] or token_hex(10),
        private_key=data['private_key'] or token_hex(20),
    )
    conn = await request['conn_manager'].get_connection()
    v = await conn.execute((
        pg_insert(sa_companies)
        .values(**data)
        .on_conflict_do_nothing()
        .returning(sa_companies.c.id, sa_companies.c.public_key, sa_companies.c.private_key, sa_companies.c.name)
    ))
    new_company = await v.first()
    if new_company is None:
        raise HTTPBadRequestJson(
            status='duplicate',
            details='the supplied data conflicts with an existing company',
        )
    else:
        logger.info('created company "%s", id %d, public key %s, private key %s',
                    new_company.name, new_company.id, new_company.public_key, new_company.private_key)
        if existing_company:
            await request.app['worker'].update_contractors(dict(new_company))
        return pretty_json_response(
            status_=201,
            status='success',
            details={
                'name': new_company.name,
                'public_key': new_company.public_key,
                'private_key': new_company.private_key,
            }
        )


async def company_list(request):
    """
    List companies.
    """
    c = sa_companies.c
    q = select([c.id, c.name, c.name_display, c.public_key, c.private_key]).limit(1000)

    conn = await request['conn_manager'].get_connection()
    results = [dict(r) async for r in conn.execute(q)]
    return pretty_json_response(list_=results)


async def contractor_set(request):
    """
    Create or update a contractor.
    """
    action = await _contractor_set(
        conn=await request['conn_manager'].get_connection(),
        worker=request.app['worker'],
        company=request['company'],
        data=request['json_obj'],
    )
    if action == Action.deleted:
        return pretty_json_response(
            status='success',
            details='contractor deleted',
        )
    else:
        return pretty_json_response(
            status_=201 if action == Action.created else 200,
            status='success',
            details=f'contractor {action}',
        )


SORT_OPTIONS = {
    'update': sa_contractors.c.last_updated.desc(),
    'name': sa_contractors.c.first_name.asc(),
    # TODO some configurable sort index
}
PAGINATION = 20


def _slugify(name):
    name = (name or '').replace(' ', '-').lower()
    return re.sub('[^a-z\-]', '', name)


def _get_name(name_display, row):
    name = row.first_name
    if name_display != NameOptions.first_name and row.last_name:
        if name_display == NameOptions.first_name_initial:
            name += ' ' + row.last_name[0]
        elif name_display == NameOptions.full_name:
            name += ' ' + row.last_name
    return name


def _photo_url(request, con, thumb):
    ext = '.thumb.jpg' if thumb else '.jpg'
    return request.app['media_url'] + '/' + request['company'].public_key + '/' + str(con.id) + ext


def _route_url(request, view_name, **kwargs):
    uri = request.app.router[view_name].url_for(**kwargs)
    return '{}{}'.format(request.app['root_url'], uri)


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
    q = (
        select([c.id, c.first_name, c.last_name, c.tag_line, c.primary_description, c.town, c.country])
        .where(c.company == request['company'].id)
        .order_by(sort_on)
        .offset(offset)
        .limit(PAGINATION)
    )
    results = []
    name_display = request['company'].name_display

    conn = await request['conn_manager'].get_connection()
    async for con in conn.execute(q):
        name = _get_name(name_display, con)
        results.append(dict(
            id=con.id,
            url=_route_url(request, 'contractor-get', company=request['company'].public_key, id=con.id),
            link='{}-{}'.format(con.id, _slugify(name)),
            name=name,
            tag_line=con.tag_line,
            primary_description=con.primary_description,
            town=con.town,
            country=con.country,
            photo=_photo_url(request, con, True),
        ))
    return public_json_response(list_=results)


def _group_skills(skills):
    for sub_cat, g in groupby(skills, attrgetter('subjects_name', 'subjects_category')):
        yield {
            'subject': sub_cat[0],
            'category': sub_cat[1],
            'qual_levels': [s.qual_levels_name for s in g]
        }


async def _get_skills(conn, con_id):
    cols = sa_subjects.c.category, sa_subjects.c.name, sa_qual_levels.c.name, sa_qual_levels.c.ranking
    skills_curr = await conn.execute(
        select(cols, use_labels=True)
        .select_from(
            sa_con_skills
            .join(sa_subjects, sa_con_skills.c.subject == sa_subjects.c.id)
            .join(sa_qual_levels, sa_con_skills.c.qual_level == sa_qual_levels.c.id)
        )
        .where(sa_con_skills.c.contractor == con_id)
        .order_by(sa_subjects.c.name, sa_qual_levels.c.ranking)
    )
    skills = await skills_curr.fetchall()
    return list(_group_skills(skills))


async def contractor_get(request):
    c = sa_contractors.c
    cols = c.id, c.first_name, c.last_name, c.tag_line, c.primary_description, c.extra_attributes, c.town, c.country
    con_id = request.match_info['id']
    conn = await request['conn_manager'].get_connection()
    curr = await conn.execute(
        select(cols)
        .where(and_(c.company == request['company'].id, c.id == con_id))
        .limit(1)
    )
    con = await curr.first()

    return public_json_response(
        id=con.id,
        name=_get_name(request['company'].name_display, con),
        tag_line=con.tag_line,
        primary_description=con.primary_description,
        town=con.town,
        country=con.country,
        photo=_photo_url(request, con, False),
        extra_attributes=con.extra_attributes,
        skills=await _get_skills(conn, con_id)
    )


async def enquiry(request):
    company = dict(request['company'])
    if request.method == METH_POST:
        data = request['json_obj']
        x_forward_for = request.headers.get('X-Forward-For')
        data.update(
            user_agent=request.headers.get('User-Agent'),
            ip_address=x_forward_for and x_forward_for.split(',', 1)[0].strip(' '),
            http_referrer=data.get('http_referrer') or request.headers.get('Referer'),
        )
        await request.app['worker'].submit_enquiry(company, data)
        return public_json_response(status='enquiry submitted to TutorCruncher')
    else:
        redis_pool = await request.app['worker'].get_redis_pool()
        async with redis_pool.get() as redis:
            raw_enquiry_options = await redis.get(b'enquiry-data-%d' % company['id'])
        if raw_enquiry_options:
            enquiry_options = json.loads(raw_enquiry_options.decode())
            last_updated = enquiry_options.pop('last_updated')
            update_enquiry_options = (timestamp() - last_updated) > 3600
        else:
            # no enquiry options yet exist, we have to get them now even though it will make the request slow
            update_enquiry_options = True
            enquiry_options = await request.app['worker'].get_enquiry_options(company)
        update_enquiry_options and await request.app['worker'].update_enquiry_options(company)

        return public_json_response(**enquiry_options)

