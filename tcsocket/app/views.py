import json
import re
from itertools import groupby
from operator import attrgetter, itemgetter
from typing import Any, Callable

from aiohttp import web_exceptions
from aiohttp.hdrs import METH_POST
from aiohttp.web import Response
from arq.utils import timestamp
from sqlalchemy import String, cast, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.sql import and_, or_
from yarl import URL

from .logs import logger
from .models import (Action, NameOptions, sa_companies, sa_con_skills, sa_contractors, sa_labels, sa_qual_levels,
                     sa_subjects)
from .processing import contractor_set as _contractor_set
from .utils import HTTPBadRequestJson, json_response
from .validation import CompanyCreateModal, CompanyUpdateModel, ContractorModel, DisplayMode, RouterMode

EXTRA_ATTR_TYPES = 'checkbox', 'text_short', 'text_extended', 'integer', 'stars', 'dropdown', 'datetime', 'date'
MISSING = object()
VISIBLE_FIELDS = 'client_name', 'client_email', 'client_phone', 'service_recipient_name'


async def index(request):
    return Response(text=request.app['index_html'], content_type='text/html')


ROBOTS = """\
User-agent: *
Allow: /
"""


async def robots_txt(request):
    return Response(text=ROBOTS, content_type='text/plain')


async def favicon(request):
    raise web_exceptions.HTTPMovedPermanently('https://secure.tutorcruncher.com/favicon.ico')


async def company_create(request):
    """
    Create a new company.

    Authentication and json parsing are done by middleware.
    """
    company: CompanyCreateModal = request['model']
    existing_company = bool(company.private_key)
    data = company.dict(exclude={'url'})
    data['domains'] = company.url and [URL(company.url).host]  # TODO here for backwards compatibility, to be removed

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
        return json_response(
            request,
            status_=201,
            status='success',
            details={
                'name': new_company.name,
                'public_key': new_company.public_key,
                'private_key': new_company.private_key,
            }
        )


async def company_update(request):
    """
    Modify a company.
    """
    company: CompanyUpdateModel = request['model']
    data = company.dict(include={'name', 'public_key', 'private_key', 'name_display'})
    data = {k: v for k, v in data.items() if v is not None}
    if company.domains != 'UNCHANGED':
        data['domains'] = company.domains

    options = company.dict(include={'show_stars', 'display_mode', 'router_mode', 'show_hours_reviewed', 'show_labels'})
    options = {k: v for k, v in options.items() if v is not None}
    if options:
        data['options'] = options

    conn = await request['conn_manager'].get_connection()
    public_key = request['company'].public_key
    c = sa_companies.c
    if data:
        await conn.execute((
            update(sa_companies)
            .values(**data)
            .where(c.public_key == public_key)
        ))
        logger.info('company "%s" updated, %s', public_key, data)

    select_fields = c.id, c.public_key, c.private_key, c.name_display, c.domains
    q = select(select_fields).where(c.public_key == public_key)
    result = await conn.execute(q)
    company = dict(await result.first())

    await request.app['worker'].update_contractors(company)
    return json_response(
        request,
        status_=200,
        status='success',
        details=data,
        company_domains=company['domains'],
    )


async def company_list(request):
    """
    List companies.
    """
    c = sa_companies.c
    q = select([c.id, c.name, c.name_display, c.domains, c.public_key, c.private_key, c.options]).limit(1000)

    conn = await request['conn_manager'].get_connection()
    results = [dict(r) async for r in conn.execute(q)]
    return json_response(request, list_=results)


async def company_options(request):
    """
    Get a companies options
    """
    options = request['company'].options or {}
    options.update(
    )
    return json_response(
        request,
        name=request['company'].name,
        name_display=request['company'].name_display or NameOptions.first_name_initial,
        show_stars=options.get('show_stars') or False,
        display_mode=options.get('display_mode') or DisplayMode.grid,
        router_mode=options.get('router_mode') or RouterMode.hash,
        show_hours_reviewed=options.get('show_hours_reviewed') or False,
        show_labels=options.get('show_labels') or False,
    )


async def contractor_set(request):
    """
    Create or update a contractor.
    """
    contractor: ContractorModel = request['model']
    action = await _contractor_set(
        conn=await request['conn_manager'].get_connection(),
        worker=request.app['worker'],
        company=request['company'],
        contractor=contractor,
    )
    if action == Action.deleted:
        return json_response(
            request,
            status='success',
            details='contractor deleted',
        )
    else:
        return json_response(
            request,
            status_=201 if action == Action.created else 200,
            status='success',
            details=f'contractor {action}',
        )


DISTANCE_SORT = '__distance__'
SORT_OPTIONS = {
    'update': sa_contractors.c.last_updated,
    'name': sa_contractors.c.first_name,
    'distance': DISTANCE_SORT,
    # TODO some configurable sort index
}
SORT_REVERSE = {
    'update': True
}
PAGINATION = 100


def _slugify(name):
    name = (name or '').replace(' ', '-').lower()
    return re.sub('[^a-z\-]', '', name)


def _get_name(name_display, row):
    name = row.first_name or ''
    if name_display != NameOptions.first_name and row.last_name:
        if name_display == NameOptions.first_name_initial:
            name += ' ' + row.last_name[0]
        else:
            name += ' ' + row.last_name
    return name


def _photo_url(request, con, thumb):
    ext = '.thumb.jpg' if thumb else '.jpg'
    return request.app['settings'].media_url + '/' + request['company'].public_key + '/' + str(con.id) + ext


def _route_url(request, view_name, **kwargs):
    uri = request.app.router[view_name].url_for(**kwargs)
    return '{}{}'.format(request.app['settings'].root_url, uri)


def _get_arg(request, field, *, decoder: Callable[[str], Any]=int, default: Any=None):
    v = request.GET.get(field, default)
    try:
        return None if v is None else decoder(v)
    except ValueError:
        raise HTTPBadRequestJson(
            status='invalid_argument',
            details=f'"{field}" had an invalid value "{v}"',
        )


async def contractor_list(request):
    sort_col = SORT_OPTIONS.get(request.GET.get('sort'), SORT_OPTIONS['update'])
    sort_reverse = SORT_REVERSE.get(request.GET.get('sort'), False)
    page = _get_arg(request, 'page', default=1)
    offset = (page - 1) * PAGINATION

    c = sa_contractors.c
    fields = c.id, c.first_name, c.last_name, c.tag_line, c.primary_description, c.town, c.country
    where = c.company == request['company'].id,

    subject_filter = _get_arg(request, 'subject')
    qual_level_filter = _get_arg(request, 'qual_level')

    select_from = None
    if subject_filter or qual_level_filter:
        select_from = sa_contractors.join(sa_con_skills)
        if subject_filter:
            select_from = select_from.join(sa_subjects)
            where += sa_subjects.c.id == subject_filter,
        if qual_level_filter:
            select_from = select_from.join(sa_qual_levels)
            where += sa_qual_levels.c.id == qual_level_filter,

    labels_filter = request.GET.getall('label', [])
    labels_exclude_filter = request.GET.getall('label_exclude', [])
    if labels_filter:
        where += c.labels.contains(cast(labels_filter, ARRAY(String(255)))),
    if labels_exclude_filter:
        where += or_(~c.labels.overlap(cast(labels_exclude_filter, ARRAY(String(255)))), c.labels.is_(None)),

    lat = _get_arg(request, 'latitude', decoder=float)
    lng = _get_arg(request, 'longitude', decoder=float)
    max_distance = _get_arg(request, 'max_distance', default=80_000)

    inc_distance = None
    if lat is not None and lng is not None:
        inc_distance = True
        request_loc = func.ll_to_earth(lat, lng)
        con_loc = func.ll_to_earth(c.latitude, c.longitude)
        distance_func = func.earth_distance(request_loc, con_loc)
        where += distance_func < max_distance,
        fields += distance_func.label('distance'),
        if sort_col == DISTANCE_SORT:
            sort_col = distance_func
    elif sort_col == DISTANCE_SORT:
        raise HTTPBadRequestJson(
            status='invalid_argument',
            details=f'distance sorting not available if latitude and longitude are not provided',
        )

    sort_on = sort_col.desc() if sort_reverse else sort_col.asc()
    q = (
        select(fields)
        .where(and_(*where)).order_by(sort_col)
        .order_by(sort_on, c.id)
        .distinct(sort_col, c.id)
        .offset(offset)
        .limit(PAGINATION)
    )
    if select_from is not None:
        q = q.select_from(select_from)
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
            distance=inc_distance and int(con.distance),
        ))
    return json_response(request, list_=results)


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
            sa_con_skills.join(sa_subjects).join(sa_qual_levels)
        )
        .where(sa_con_skills.c.contractor == con_id)
        .order_by(sa_subjects.c.name, sa_qual_levels.c.ranking)
    )
    skills = await skills_curr.fetchall()
    return list(_group_skills(skills))


async def contractor_get(request):
    c = sa_contractors.c
    cols = (
        c.id, c.first_name, c.last_name, c.tag_line, c.primary_description, c.extra_attributes, c.town,
        c.country, c.labels
    )
    con_id = request.match_info['id']
    conn = await request['conn_manager'].get_connection()
    curr = await conn.execute(
        select(cols)
        .where(and_(c.company == request['company'].id, c.id == con_id))
        .limit(1)
    )
    con = await curr.first()

    return json_response(
        request,
        id=con.id,
        name=_get_name(request['company'].name_display, con),
        tag_line=con.tag_line,
        primary_description=con.primary_description,
        town=con.town,
        country=con.country,
        photo=_photo_url(request, con, False),
        extra_attributes=con.extra_attributes,
        skills=await _get_skills(conn, con_id),
        labels=con.labels or [],
    )


async def _sub_qual_list(request, q):
    q = q.where(sa_contractors.c.company == request['company'].id)
    conn = await request['conn_manager'].get_connection()
    return json_response(
        request,
        list_=[dict(s, link=f'{s.id}-{_slugify(s.name)}') async for s in conn.execute(q)]
    )


async def subject_list(request):
    q = (
        select([sa_subjects.c.id, sa_subjects.c.name, sa_subjects.c.category])
        .select_from(sa_con_skills.join(sa_contractors).join(sa_subjects))
        .order_by(sa_subjects.c.category, sa_subjects.c.id)
        .distinct(sa_subjects.c.category, sa_subjects.c.id)
    )
    return await _sub_qual_list(request, q)


async def qual_level_list(request):
    q = (
        select([sa_qual_levels.c.id, sa_qual_levels.c.name])
        .select_from(sa_con_skills.join(sa_contractors).join(sa_qual_levels))
        .order_by(sa_qual_levels.c.ranking, sa_qual_levels.c.id)
        .distinct(sa_qual_levels.c.ranking, sa_qual_levels.c.id)
    )
    return await _sub_qual_list(request, q)


async def labels_list(request):
    q = (
        select([sa_labels.c.name, sa_labels.c.machine_name])
        .where(sa_labels.c.company == request['company'].id)
    )
    conn = await request['conn_manager'].get_connection()
    return json_response(
        request,
        **{s.machine_name: s.name async for s in conn.execute(q)}
    )


FIELD_TYPE_LOOKUP = {
    'field': 'id',
    'string': 'text',
    'email': 'email',
    'choice': 'select',
    'boolean': 'checkbox',
    'integer': None,
    'date': None,
    'datetime': None,
}


def _convert_field(name, value, prefix=None):
    value_ = dict(value)
    ftype = FIELD_TYPE_LOOKUP[value_.pop('type')]
    if ftype is None:
        return None
    value_.pop('read_only')
    return dict(
        field=name,
        type=ftype,
        prefix=prefix,
        **value_
    )


async def enquiry(request):
    company = dict(request['company'])
    if request.method == METH_POST:
        data = request['model'].dict()
        data = {k: v for k, v in data.items() if v is not None}
        x_forward_for = request.headers.get('X-Forwarded-For')
        referrer = request.headers.get('Referer')
        data.update(
            user_agent=request.headers.get('User-Agent'),
            ip_address=x_forward_for and x_forward_for.split(',', 1)[0].strip(' '),
            http_referrer=referrer and referrer[:1023],
        )
        await request.app['worker'].submit_enquiry(company, data)
        return json_response(request, status='enquiry submitted to TutorCruncher', status_=201)
    else:
        redis = await request.app['worker'].get_redis()
        raw_enquiry_options = await redis.get(b'enquiry-data-%d' % company['id'])
        if raw_enquiry_options:
            enquiry_options_ = json.loads(raw_enquiry_options.decode())
            last_updated = enquiry_options_['last_updated']
            update_enquiry_options = (timestamp() - last_updated) > 3600
        else:
            # no enquiry options yet exist, we have to get them now even though it will make the request slow
            enquiry_options_ = await request.app['worker'].get_enquiry_options(company)
            last_updated = 0
            update_enquiry_options = True
        update_enquiry_options and await request.app['worker'].update_enquiry_options(company)

        # make the enquiry form data easier to render for js
        visible = filter(bool, [
            _convert_field(f, enquiry_options_[f]) for f in VISIBLE_FIELDS
        ] + [
            _convert_field(k, v, 'attributes') for k, v in enquiry_options_['attributes'].get('children', {}).items()
        ])

        enquiry_options = {
            'visible': sorted(visible, key=itemgetter('sort_index', )),
            'hidden': {
                'contractor': _convert_field('contractor', enquiry_options_['contractor']),
            },
            'last_updated': last_updated,
        }
        return json_response(request, **enquiry_options)
