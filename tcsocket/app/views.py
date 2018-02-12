import json
import re
from datetime import date, datetime
from enum import Enum
from itertools import groupby
from operator import attrgetter, itemgetter
from typing import Any, Callable

import pydantic
from aiohttp import web_exceptions
from aiohttp.hdrs import METH_POST
from aiohttp.web import Response
from arq.utils import timestamp
from sqlalchemy import String, cast, func, select, update
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import and_, distinct, or_
from sqlalchemy.sql.functions import count as count_func
from yarl import URL

from .geo import geocode, get_ip
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


REDIS_ENQUIRY_CACHE_KEY = b'enquiry-data-%d'


async def clear_enquiry(request):
    redis = request.app['redis']
    v = await redis.delete(REDIS_ENQUIRY_CACHE_KEY % request['company'].id)
    return json_response(
        request,
        status='success',
        data_existed=bool(v)
    )


DISTANCE_SORT = '__distance__'
SORT_OPTIONS = {
    'update': sa_contractors.c.last_updated,
    'name': sa_contractors.c.first_name,
    'distance': DISTANCE_SORT,
    # TODO some configurable sort index
}


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


async def contractor_list(request):  # noqa: C901 (ignore complexity)
    sort_val = request.GET.get('sort')
    sort_col = SORT_OPTIONS.get(sort_val, SORT_OPTIONS['update'])
    page = _get_arg(request, 'page', default=1)
    pagination = min(_get_arg(request, 'pagination', default=100), 100)
    offset = (page - 1) * pagination

    company = request['company']
    options = company.options or {}
    c = sa_contractors.c
    fields = c.id, c.first_name, c.last_name, c.tag_line, c.primary_description, c.town, c.country
    show_labels = options.get('show_labels')
    if show_labels:
        fields += c.labels,

    show_stars = options.get('show_stars')
    if show_stars:
        fields += c.review_rating,

    show_hours_reviewed = options.get('show_hours_reviewed')
    if show_hours_reviewed:
        fields += c.review_duration,

    where = c.company == company.id,

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

    location = await geocode(request)
    data = {}
    inc_distance = None
    if location:
        data['location'] = location
        max_distance = _get_arg(request, 'max_distance', default=80_000)
        inc_distance = True
        request_loc = func.ll_to_earth(location['lat'], location['lng'])
        con_loc = func.ll_to_earth(c.latitude, c.longitude)
        distance_func = func.earth_distance(request_loc, con_loc)
        where += distance_func < max_distance,
        fields += distance_func.label('distance'),
        if not sort_val:
            sort_col = DISTANCE_SORT
        if sort_col == DISTANCE_SORT:
            sort_col = distance_func
    elif sort_col == DISTANCE_SORT:
        raise HTTPBadRequestJson(
            status='invalid_argument',
            details=f'distance sorting not available if latitude and longitude are not provided',
        )

    sort_on = sort_col.desc() if sort_col == sa_contractors.c.last_updated else sort_col.asc()
    q_iter = (
        select(fields)
        .where(and_(*where))
        .order_by(sort_on, c.id)
        .distinct(sort_col, c.id)
        .offset(offset)
        .limit(pagination)
    )
    q_count = select([count_func(distinct(c.id))]).where(and_(*where))
    if select_from is not None:
        q_iter = q_iter.select_from(select_from)
        q_count = q_count.select_from(select_from)

    results = []
    name_display = company.name_display
    conn = await request['conn_manager'].get_connection()
    async for row in conn.execute(q_iter):
        name = _get_name(name_display, row)
        con = dict(
            id=row.id,
            url=_route_url(request, 'contractor-get', company=company.public_key, id=row.id),
            link='{}-{}'.format(row.id, _slugify(name)),
            name=name,
            tag_line=row.tag_line,
            primary_description=row.primary_description,
            town=row.town,
            country=row.country,
            photo=_photo_url(request, row, True),
            distance=inc_distance and int(row.distance),
        )
        if show_labels:
            con['labels'] = row.labels or []
        if show_stars:
            con['review_rating'] = row.review_rating
        if show_hours_reviewed:
            con['review_duration'] = row.review_duration
        results.append(con)

    cur_count = await conn.execute(q_count)
    return json_response(
        request,
        location=location,
        results=results,
        count=(await cur_count.first())[0],
    )


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
        c.country, c.labels, c.review_rating, c.review_duration
    )
    con_id = request.match_info['id']
    conn = await request['conn_manager'].get_connection()
    curr = await conn.execute(
        select(cols)
        .where(and_(c.company == request['company'].id, c.id == con_id))
        .limit(1)
    )
    con = await curr.first()
    options = request['company'].options or {}
    return json_response(
        request,
        id=con.id,
        name=_get_name(request['company'].name_display, con),
        tag_line=con.tag_line,
        primary_description=con.primary_description,
        town=con.town,
        country=con.country,
        photo=_photo_url(request, con, False),
        extra_attributes=con.extra_attributes and sorted(con.extra_attributes, key=lambda e: e.get('sort_index', 1000)),
        skills=await _get_skills(conn, con_id),
        labels=con.labels if (options.get('show_labels') and con.labels) else [],
        review_rating=con.review_rating if options.get('show_stars') else None,
        review_duration=con.review_duration if options.get('show_hours_reviewed') else None,
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


async def enquiry(request):
    company = dict(request['company'])

    redis = request.app['redis']
    redis_key = REDIS_ENQUIRY_CACHE_KEY % company['id']
    raw_enquiry_options = await redis.get(redis_key)
    ts = timestamp()
    if raw_enquiry_options:
        enquiry_options = json.loads(raw_enquiry_options.decode())
        enquiry_last_updated = enquiry_options['last_updated']
        # 1800 so data should never expire for regularly used forms
        if (ts - enquiry_last_updated) > 1800:
            await request.app['worker'].update_enquiry_options(company)
    else:
        # no enquiry options yet exist, we have to get them now even though it will make the request slow
        enquiry_options = await request.app['worker'].get_enquiry_options(company)
        enquiry_options['last_updated'] = ts
        await redis.setex(redis_key, 3600, json.dumps(enquiry_options).encode())

    enq_method = enquiry_post if request.method == METH_POST else enquiry_get
    return await enq_method(request, company, enquiry_options)


FIELD_TYPE_LOOKUP = {
    'field': 'id',
    'string': 'text',
    'email': 'email',
    'choice': 'select',
    'boolean': 'checkbox',
    'integer': 'integer',
    'date': 'date',
    'datetime': 'datetime',
}
CREATE_ENUM = object()
FIELD_VALIDATION_LOOKUP = {
    'string': str,
    'email': pydantic.EmailStr,
    'choice': CREATE_ENUM,
    'boolean': bool,
    'integer': int,
    'date': date,
    'datetime': datetime,
}


class AttributeBaseModel(pydantic.BaseModel):
    @pydantic.validator('*')
    def make_serializable(cls, v):
        # datetime is a subclass of date
        if isinstance(v, date):
            return v.isoformat()
        elif isinstance(v, Enum):
            return v.value
        else:
            return v


async def enquiry_post(request, company, enquiry_options):
    data = request['model'].dict()
    data = {k: v for k, v in data.items() if v is not None}
    attributes = data.pop('attributes', None)
    referrer = request.headers.get('Referer')
    data.update(
        user_agent=request.headers.get('User-Agent'),
        ip_address=get_ip(request),
        http_referrer=referrer and referrer[:1023],
    )

    fields = {}
    for name, field_data in enquiry_options['attributes'].get('children', {}).items():
        field_type = FIELD_VALIDATION_LOOKUP[field_data['type']]
        if field_type == CREATE_ENUM:
            field_type = Enum('DynamicEnum', {f'v{i}': c['value'] for i, c in enumerate(field_data['choices'])})
        fields[name] = (field_type, ... if field_data['required'] else None)

    if fields:
        dynamic_model = pydantic.create_model('AttributeModel', **fields, __base__=AttributeBaseModel)
        try:
            attributes = dynamic_model.parse_obj(attributes)
        except pydantic.ValidationError as e:
            raise HTTPBadRequestJson(status='invalid attribute data', details=e.errors_dict)
        else:
            data['attributes'] = {k: v for k, v in attributes.dict().items() if v is not None}

    await request.app['worker'].submit_enquiry(company, data)
    return json_response(request, status='enquiry submitted to TutorCruncher', status_=201)


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


async def enquiry_get(request, company, enquiry_options):
    # make the enquiry form data easier to render for js
    visible = filter(bool, [
        _convert_field(f, enquiry_options[f]) for f in VISIBLE_FIELDS
    ] + [
        _convert_field(k, v, 'attributes') for k, v in enquiry_options['attributes'].get('children', {}).items()
    ])

    return json_response(
        request,
        visible=sorted(visible, key=itemgetter('sort_index', )),
        hidden={'contractor': _convert_field('contractor', enquiry_options['contractor'])},
    )
