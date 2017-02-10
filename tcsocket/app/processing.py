from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import and_, or_

from .logs import logger
from .models import Action, sa_con_skills, sa_contractors, sa_qual_levels, sa_subjects
from .utils import HTTPForbiddenJson, HTTPNotFoundJson


async def _set_skills(conn, contractor_id, skills):
    """
    create missing subjects and qualification levels, then create contractor skills for them.
    """
    if not skills:
        # just delete skills and return
        await conn.execute(sa_con_skills.delete().where(sa_con_skills.c.contractor == contractor_id))
        return
    async with conn.begin():
        # get ids of subjects, creating them if necessary
        subject_cols = sa_subjects.c.id, sa_subjects.c.name, sa_subjects.c.category
        cur = await conn.execute(
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
            cur = await conn.execute(sa_subjects.insert().values(subjects_to_create).returning(*subject_cols))
            subjects.update({(r[1], r[2]): r[0] async for r in cur})

        # get ids of qualification levels, creating them if necessary
        qual_level_cols = sa_qual_levels.c.id, sa_qual_levels.c.name
        cur = await conn.execute(
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
            cur = await conn.execute(sa_qual_levels.insert().values(qual_levels_to_create).returning(*qual_level_cols))
            qual_levels.update({r[1]: r[0] async for r in cur})

        # skills the contractor should have
        con_skills = {(subjects[(s['subject'], s['category'])], qual_levels[s['qual_level']]) for s in skills}

        q = (
            select([sa_con_skills.c.id, sa_con_skills.c.subject, sa_con_skills.c.qual_level])
            .where(sa_con_skills.c.contractor == contractor_id)
        )
        to_delete = set()
        async for r in conn.execute(q):
            key = r.subject, r.qual_level
            try:
                con_skills.remove(key)
            except KeyError:
                # skill doesn't exist in con_skills, it should be deleted
                to_delete.add(r.id)

        to_delete and await conn.execute(sa_con_skills.delete().where(sa_con_skills.c.id.in_(to_delete)))

        if con_skills:
            q = sa_con_skills.insert().values([
                dict(contractor=contractor_id, subject=subject, qual_level=qual_level)
                for subject, qual_level in con_skills
            ])
            await conn.execute(q)


def get_special_extra_attr(extra_attributes, machine_name, attr_type):
    eas = [ea for ea in extra_attributes if ea['type'] == attr_type]
    if eas:
        eas.sort(key=lambda ea: (ea['machine_name'] != machine_name, ea['sort_index']))
        ea = eas[0]
        return ea['value'], [ea_ for ea_ in extra_attributes if ea_['id'] != ea['id']]
    else:
        return None, extra_attributes


async def contractor_set(*, conn, company, worker, data) -> Action:
    """
    Create or update a contractor.

    :param conn: pg connection
    :param company: dict with company info, including id and public_key
    :param worker: instance of RequestWorker
    :param data: data about contractor
    :return: Action: created, updated or deleted
    """
    con_id = data.pop('id')
    deleted = data.pop('deleted')
    if deleted:
        curr = await conn.execute(
            sa_contractors
            .delete()
            .where(and_(sa_contractors.c.company == company['id'], sa_contractors.c.id == con_id))
            .returning(sa_contractors.c.id)
        )
        if not await curr.first():
            raise HTTPNotFoundJson(
                status='not found',
                details=f'contractor with id {con_id} not found',
            )
        return Action.deleted

    skills = data.pop('skills')
    photo = data.pop('photo', None)
    location = data.pop('location', None)
    if location:
        data.update(location)

    ex_attrs = data.pop('extra_attributes')
    tag_line, ex_attrs = get_special_extra_attr(ex_attrs, 'tag_line', 'text_short')
    primary_description, ex_attrs = get_special_extra_attr(ex_attrs, 'primary_description', 'text_extended')
    data.update(
        last_updated=data.get('last_updated') or datetime.now(),
        extra_attributes=ex_attrs,
        tag_line=tag_line,
        primary_description=primary_description,
    )
    v = await conn.execute(
        pg_insert(sa_contractors)
        .values(id=con_id, company=company['id'], action=Action.created, **data)
        .on_conflict_do_update(
            index_elements=[sa_contractors.c.id],
            where=sa_contractors.c.company == company['id'],
            set_=dict(action=Action.updated, **data)
        )
        .returning(sa_contractors.c.action)
    )
    r = await v.first()
    if r is None:
        # the contractor already exists but on another company
        raise HTTPForbiddenJson(
            status='permission denied',
            details=f'you do not have permission to update contractor {con_id}',
        )
    await _set_skills(conn, con_id, skills)
    photo and await worker.get_image(company['public_key'], con_id, photo)
    logger.info('%s contractor on %s', r.action, company['public_key'])
    return r.action
