import logging
from itertools import groupby
from operator import attrgetter

from sqlalchemy import String, cast, func, select
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.sql import and_, distinct
from sqlalchemy.sql import functions as sql_f
from sqlalchemy.sql import or_

from ..geo import geocode
from ..models import Action, NameOptions, sa_con_skills, sa_contractors, sa_qual_levels, sa_subjects
from ..processing import contractor_set as _contractor_set
from ..utils import HTTPNotFoundJson, get_arg, get_pagination, json_response, route_url, slugify
from ..validation import ContractorModel

logger = logging.getLogger('socket.views')


async def contractor_set(request):
    """
    Create or update a contractor.
    """
    contractor: ContractorModel = request['model']
    action = await _contractor_set(
        conn=await request['conn_manager'].get_connection(),
        redis=request.app['redis'],
        company=request['company'],
        contractor=contractor,
    )
    if action == Action.deleted:
        return json_response(request, status='success', details='contractor deleted',)
    else:
        return json_response(
            request, status_=201 if action == Action.created else 200, status='success', details=f'contractor {action}',
        )


c = sa_contractors.c
SORT_OPTIONS = {
    'last_updated': c.last_updated,
    'review_rating': c.review_rating,
    'name': c.first_name,
}


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
    return f'{request.app["settings"].media_url}/{request["company"].public_key}/{con.id}{ext}?h={con.photo_hash}'


async def contractor_list(request):  # noqa: C901 (ignore complexity)
    sort_val = request.query.get('sort')
    sort_col = SORT_OPTIONS.get(sort_val, SORT_OPTIONS['last_updated'])

    pagination, offset = get_pagination(request, 100, 100)

    company = request['company']
    options = company.options or {}
    fields = (
        c.id,
        c.first_name,
        c.last_name,
        c.tag_line,
        c.primary_description,
        c.town,
        c.country,
        c.photo_hash,
    )
    show_labels = options.get('show_labels')
    if show_labels:
        fields += (c.labels,)

    show_stars = options.get('show_stars')
    if show_stars:
        fields += (c.review_rating,)

    show_hours_reviewed = options.get('show_hours_reviewed')
    if show_hours_reviewed:
        fields += (c.review_duration,)

    where = (c.company == company.id,)

    subject_filter = get_arg(request, 'subject')
    qual_level_filter = get_arg(request, 'qual_level')

    select_from = None
    if subject_filter or qual_level_filter:
        select_from = sa_contractors.join(sa_con_skills)
        if subject_filter:
            select_from = select_from.join(sa_subjects)
            where += (sa_subjects.c.id == subject_filter,)
        if qual_level_filter:
            select_from = select_from.join(sa_qual_levels)
            where += (sa_qual_levels.c.id == qual_level_filter,)

    labels_filter = request.query.getall('label', [])
    labels_exclude_filter = request.query.getall('label_exclude', [])
    if labels_filter:
        where += (c.labels.contains(cast(labels_filter, ARRAY(String(255)))),)
    if labels_exclude_filter:
        where += (or_(~c.labels.overlap(cast(labels_exclude_filter, ARRAY(String(255)))), c.labels.is_(None)),)

    location = await geocode(request)
    inc_distance = None
    if location:
        if location.get('error'):
            return json_response(request, location=location, results=[], count=0,)
        max_distance = get_arg(request, 'max_distance', default=80_000)
        inc_distance = True
        request_loc = func.ll_to_earth(location['lat'], location['lng'])
        con_loc = func.ll_to_earth(c.latitude, c.longitude)
        distance_func = func.earth_distance(request_loc, con_loc)
        where += (distance_func < max_distance,)
        fields += (distance_func.label('distance'),)
        sort_col = distance_func

    distinct_cols = sort_col, c.id
    if sort_col == c.review_rating:
        sort_on = c.review_rating.desc().nullslast(), c.review_duration.desc().nullslast(), c.id
        distinct_cols = c.review_rating, c.review_duration, c.id
    elif sort_col == c.last_updated:
        sort_on = sort_col.desc(), c.id
    else:
        sort_on = sort_col.asc(), c.id

    q_iter = (
        select(fields).where(and_(*where)).order_by(*sort_on).distinct(*distinct_cols).offset(offset).limit(pagination)
    )
    q_count = select([sql_f.count(distinct(c.id))]).where(and_(*where))
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
            url=route_url(request, 'contractor-get', company=company.public_key, id=row.id),
            link=f'{row.id}-{slugify(name)}',
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
    return json_response(request, location=location, results=results, count=(await cur_count.first())[0],)


def _group_skills(skills):
    for sub_cat, g in groupby(skills, attrgetter('subjects_name', 'subjects_category')):
        yield {'subject': sub_cat[0], 'category': sub_cat[1], 'qual_levels': [s.qual_levels_name for s in g]}


async def _get_skills(conn, con_id):
    cols = sa_subjects.c.category, sa_subjects.c.name, sa_qual_levels.c.name, sa_qual_levels.c.ranking
    skills_curr = await conn.execute(
        select(cols, use_labels=True)
        .select_from(sa_con_skills.join(sa_subjects).join(sa_qual_levels))
        .where(sa_con_skills.c.contractor == con_id)
        .order_by(sa_subjects.c.name, sa_qual_levels.c.ranking)
    )
    skills = await skills_curr.fetchall()
    return list(_group_skills(skills))


async def contractor_get(request):
    cols = (
        c.id,
        c.first_name,
        c.last_name,
        c.tag_line,
        c.primary_description,
        c.extra_attributes,
        c.town,
        c.country,
        c.labels,
        c.review_rating,
        c.review_duration,
        c.photo_hash,
    )
    con_id = request.match_info['id']
    conn = await request['conn_manager'].get_connection()
    curr = await conn.execute(select(cols).where(and_(c.company == request['company'].id, c.id == con_id)).limit(1))
    con = await curr.first()
    if not con:
        raise HTTPNotFoundJson()
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
