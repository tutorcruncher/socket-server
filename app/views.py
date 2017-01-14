from datetime import datetime
from secrets import token_hex

import re
import trafaret as t
from psycopg2._psycopg import IntegrityError
from dateutil.parser import parse as dt_parse
from sqlalchemy import literal, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import and_, or_

from .models import sa_companies, sa_contractors, Action, NameOptions, sa_subjects, sa_qual_levels, sa_con_skills
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
            'latitude': t.Or(t.Float | t.Null),
            'longitude': t.Or(t.Float | t.Null),
        }),

        t.Key('extra_attributes', optional=True): t.List(ANY_DICT),
        t.Key('skills', optional=True): t.List(t.Dict({
            'subject': t.String,
            'category': t.String,
            'qual_level': t.String,
            'qual_level_ranking': t.Float,
        })),

        t.Key('image', optional=True): t.String(max_length=63),
        t.Key('last_updated', optional=True): t.String >> dt_parse,
        t.Key('photo', optional=True): t.URL,
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


async def set_skills(request, contractor_id, skills):
    """
    create missing subjects and qualification levels, then create contractor skills for them.
    """
    execute = request['conn'].execute
    async with request['conn'].begin():
        # get ids of subjects, creating them if necessary
        subject_cols = sa_subjects.c.id, sa_subjects.c.name, sa_subjects.c.category
        cur = await execute(
            select(subject_cols)
            .where(or_(*[
                and_(sa_subjects.c.name == s['subject'], sa_subjects.c.category == s['category'])
                for s in skills
            ]))
        )
        subjects = {(r.name, r.category): r.id for r in (await cur.fetchall())}

        subjects_to_create = []
        for skill in skills:
            key = skill['subject'], skill['category']
            if key not in subjects:
                subjects[key] = None  # to make sure it's not created twice
                subjects_to_create.append(dict(name=skill['subject'], category=skill['category']))

        if subjects_to_create:
            cur = await execute(sa_subjects.insert().values(subjects_to_create).returning(*subject_cols))
            subjects.update({(r[1], r[2]): r[0] async for r in cur})

        # get ids of qualification levels, creating them if necessary
        qual_level_cols = sa_qual_levels.c.id, sa_qual_levels.c.name
        cur = await execute(
            select(qual_level_cols)
            .where(sa_qual_levels.c.name.in_({s['qual_level'] for s in skills}))
        )
        qual_levels = {r.name: r.id for r in (await cur.fetchall())}

        qual_levels_to_create = []
        for skill in skills:
            ql_name = skill['qual_level']
            if ql_name not in qual_levels:
                qual_levels[ql_name] = None  # to make sure it's not created twice
                qual_levels_to_create.append(dict(name=ql_name, ranking=skill['qual_level_ranking']))

        if qual_levels_to_create:
            cur = await execute(sa_qual_levels.insert().values(qual_levels_to_create).returning(*qual_level_cols))
            qual_levels.update({r[1]: r[0] async for r in cur})

        # skills the contractor should have
        con_skills = {(subjects[(s['subject'], s['category'])], qual_levels[s['qual_level']]) for s in skills}

        q = (
            select([sa_con_skills.c.id, sa_con_skills.c.subject, sa_con_skills.c.qual_level])
            .where(sa_con_skills.c.contractor == contractor_id)
        )
        to_delete = set()
        async for r in execute(q):
            key = r.subject, r.qual_level
            if key in con_skills:
                con_skills.remove(key)
            else:
                to_delete.add(r.id)

        to_delete and await execute(sa_con_skills.delete().where(sa_con_skills.c.id.in_(to_delete)))

        if con_skills:
            q = sa_con_skills.insert().values([
                dict(contractor=contractor_id, subject=subject, qual_level=qual_level)
                for subject, qual_level in con_skills
            ])
            await execute(q)

async def contractor_set(request):
    """
    Create or update a contractor.
    """
    data = request['json_obj']
    skills = data.pop('skills', [])
    location = data.pop('location', None)
    if location:
        data.update(location)
    photo = data.pop('photo', None)
    if photo:
        # TODO deal with photo
        pass
    data['last_updated'] = data.get('last_updated') or datetime.now()
    extra_attrs = data.get('extra_attributes', [])
    data['extra_attributes'] = literal(extra_attrs, JSONB)
    cid = data.pop('id')
    company_id = request['company'].id
    v = await request['conn'].execute(
        pg_insert(sa_contractors)
        .values(id=cid, company=company_id, action=Action.insert, **data)
        .on_conflict_do_update(
            index_elements=[sa_contractors.c.id],
            where=sa_contractors.c.company == company_id,
            set_=dict(action=Action.update, **data)
        )
        .returning(sa_contractors.c.action)
    )
    status, status_text = (201, 'created') if (await v.first()).action == Action.insert else (200, 'updated')
    await set_skills(request, cid, skills)
    return json_response({
        'status': 'success',
        'details': f'contractor {status_text}',
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
    cols = c.id, c.first_name, c.last_name, c.photo, c.tag_line
    q = (
        select(cols)
        .where(c.company == request['company'].id)
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
    # TODO
    return json_response({}, request=request)
