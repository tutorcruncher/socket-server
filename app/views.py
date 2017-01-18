import re
from datetime import datetime
from secrets import token_hex

import trafaret as t
from dateutil.parser import parse as dt_parse
from sqlalchemy import literal, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import and_, or_

from .models import Action, NameOptions, sa_companies, sa_con_skills, sa_contractors, sa_qual_levels, sa_subjects
from .utils import HTTPBadRequestJson, HTTPNotFoundJson, json_response


VIEW_SCHEMAS = {
    'company-create': t.Dict({
        'name': t.String(min_length=4, max_length=63),
        t.Key('name_display', optional=True): t.Or(
            t.Atom('first_name') |
            t.Atom('first_name_initial') |
            t.Atom('full_name')
        ),
    }),
    'contractor-set': t.Dict({
        'id': t.Int(),
        t.Key('deleted', default=False): t.Bool,
        t.Key('first_name', optional=True): t.String(max_length=63),
        t.Key('last_name', optional=True): t.String(max_length=63),

        t.Key('town', optional=True): t.String(max_length=63),
        t.Key('country', optional=True): t.String(max_length=63),
        t.Key('location', optional=True): t.Dict({
            'latitude': t.Or(t.Float | t.Null),
            'longitude': t.Or(t.Float | t.Null),
        }),

        t.Key('extra_attributes', default=[]): t.List(t.Dict({
            'machine_name': t.Or(t.Null | t.String),
            'type': t.String,
            'name': t.String,
            'value': t.Or(t.Bool | t.String | t.Float),
            'id': t.Int,
            'sort_index': t.Float,
        })),

        t.Key('skills', default=[]): t.List(t.Dict({
            'subject': t.String,
            'category': t.String,
            'qual_level': t.String,
            'qual_level_ranking': t.Float,
        })),

        t.Key('last_updated', optional=True): t.String >> dt_parse,
        t.Key('photo', optional=True): t.URL,
    })
}
VIEW_SCHEMAS['contractor-set'].ignore_extra('*')


async def index(request):
    return json_response({
        'title': 'TODO',
    }, request=request)


async def company_create(request):
    """
    Create a new company.

    Authentication and json parsing are done by middleware.
    """
    data = request['json_obj']
    data['key'] = token_hex(10)
    v = await request['conn'].execute((
        pg_insert(sa_companies)
        .values(**data)
        .on_conflict_do_nothing(index_elements=[sa_companies.c.name])
        .returning(sa_companies.c.key, sa_companies.c.name)
    ))
    new_company = await v.first()
    if new_company is None:
        raise HTTPBadRequestJson(
            status='duplicate',
            details=f'company with the name "{data["name"]}" already exists',
        )
    else:
        return json_response({
            'status': 'success',
            'details': new_company
        }, request=request, status=201)


async def set_skills(request, contractor_id, skills):
    """
    create missing subjects and qualification levels, then create contractor skills for them.
    """
    execute = request['conn'].execute
    if not skills:
        # just delete skills and return
        await execute(sa_con_skills.delete().where(sa_con_skills.c.contractor == contractor_id))
        return
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
            try:
                con_skills.remove(key)
            except KeyError:
                # skill doesn't exist in con_skills, it should be deleted
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
    company_id = request['company'].id
    data = request['json_obj']
    con_id = data.pop('id')
    deleted = data.pop('deleted')
    if deleted:
        curr = await request['conn'].execute(
            sa_contractors
            .delete()
            .where(and_(sa_contractors.c.company == company_id, sa_contractors.c.id == con_id))
            .returning(sa_contractors.c.id)
        )
        if not await curr.first():
            raise HTTPNotFoundJson(
                status='not found',
                details=f'contractor with id {con_id} not found',
            )
        return json_response({
            'status': 'success',
            'details': 'contractor deleted',
        }, request=request)

    skills = data.pop('skills')
    photo = data.pop('photo', None)
    location = data.pop('location', None)
    if location:
        data.update(location)

    data['last_updated'] = data.get('last_updated', datetime.now())
    data['extra_attributes'] = literal(data['extra_attributes'], JSONB)
    v = await request['conn'].execute(
        pg_insert(sa_contractors)
        .values(id=con_id, company=company_id, action=Action.insert, **data)
        .on_conflict_do_update(
            index_elements=[sa_contractors.c.id],
            where=sa_contractors.c.company == company_id,
            set_=dict(action=Action.update, **data)
        )
        .returning(sa_contractors.c.action)
    )
    r = await v.first()
    status, status_text = (201, 'created') if r.action == Action.insert else (200, 'updated')
    await set_skills(request, con_id, skills)
    photo and await request.app['image_worker'].get_image(company_id, con_id, photo)
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
        select([c.id, c.first_name, c.last_name, c.tag_line])
        .where(c.company == request['company'].id)
        .order_by(sort_on)
        .offset(offset)
        .limit(PAGINATION)
    )
    results = []
    name_display = request['company'].name_display
    async for row in request['conn'].execute(q):
        name = _get_name(name_display, row)
        results.append(dict(
            id=row.id,
            slug=_slugify(name),
            name=name,
            tag_line=row.tag_line,
        ))
    return json_response(results, request=request, status=200)


async def contractor_get(request):
    c = sa_contractors.c
    cols = c.id, c.first_name, c.last_name, c.tag_line, c.extra_attributes
    con_id = request.match_info['id']
    curr = await request['conn'].execute(
        select(cols)
        .where(and_(c.company == request['company'].id, c.id == con_id))
        .limit(1)
    )
    con = await curr.first()

    cols = sa_subjects.c.category, sa_subjects.c.name, sa_qual_levels.c.name, sa_qual_levels.c.ranking
    skills_curr = await request['conn'].execute(
        select(cols, use_labels=True)
        .select_from(
            sa_con_skills
            .join(sa_subjects, sa_con_skills.c.subject == sa_subjects.c.id)
            .join(sa_qual_levels, sa_con_skills.c.qual_level == sa_qual_levels.c.id)
        )
        .where(sa_con_skills.c.contractor == con_id)
    )
    return json_response(dict(
        id=con.id,
        name=_get_name(request['company'].name_display, con),
        tag_line=con.tag_line,
        extra_attributes=con.extra_attributes,
        skills=[{
            'subject': r.subjects_name,
            'category': r.subjects_category,
            'qual_level': r.qual_levels_name,
            'ranking': r.qual_levels_ranking,
        } for r in (await skills_curr.fetchall())]
    ), request=request)
