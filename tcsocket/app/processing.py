from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import and_

from .logs import logger
from .models import Action, sa_con_skills, sa_contractors, sa_qual_levels, sa_subjects
from .utils import HTTPForbiddenJson, HTTPNotFoundJson


def _unique_on(iter, key):
    sofar = set()
    for item in iter:
        v = item[key]
        if v not in sofar:
            sofar.add(v)
            yield item


async def _set_skills(conn, contractor_id, skills):
    """
    create missing subjects and qualification levels, then create contractor skills for them.
    """
    if not skills:
        # just delete skills and return
        await conn.execute(sa_con_skills.delete().where(sa_con_skills.c.contractor == contractor_id))
        return
    async with conn.begin():
        await conn.execute(
            pg_insert(sa_subjects)
            .values([
                {'id': s['subject_id'], 'name': s['subject'], 'category': s['category']}
                for s in _unique_on(skills, 'subject_id')
            ])
            .on_conflict_do_nothing()
        )
        await conn.execute(
            pg_insert(sa_qual_levels)
            .values([
                {'id': s['qual_level_id'], 'name': s['qual_level'], 'ranking': s['qual_level_ranking']}
                for s in _unique_on(skills, 'qual_level_id')
            ])
            .on_conflict_do_nothing()
        )

        con_skills_to_create = {(s['subject_id'], s['qual_level_id']) for s in skills}

        q = (
            select([sa_con_skills.c.id, sa_con_skills.c.subject, sa_con_skills.c.qual_level])
            .where(sa_con_skills.c.contractor == contractor_id)
        )
        to_delete = set()
        async for r in conn.execute(q):
            key = r.subject, r.qual_level
            try:
                con_skills_to_create.remove(key)
            except KeyError:
                # skill doesn't exist in con_skills, it should be deleted
                to_delete.add(r.id)

        to_delete and await conn.execute(sa_con_skills.delete().where(sa_con_skills.c.id.in_(to_delete)))

        con_skills_to_create and await conn.execute(
            sa_con_skills.insert()
            .values([
                dict(contractor=contractor_id, subject=subject, qual_level=qual_level)
                for subject, qual_level in con_skills_to_create
            ])
        )


def _get_special_extra_attr(extra_attributes, machine_name, attr_type):
    """
    Find special extra attributes suitable for tag_line and primary_description.
    """
    eas = [ea for ea in extra_attributes if ea['type'] == attr_type]
    if eas:
        eas.sort(key=lambda ea: (ea['machine_name'] != machine_name, ea['sort_index']))
        ea = eas[0]
        return ea['value'], [ea_ for ea_ in extra_attributes if ea_['id'] != ea['id']]
    else:
        return None, extra_attributes


async def contractor_set(*, conn, company, worker, data, skip_deleted=False) -> Action:
    """
    Create or update a contractor.

    :param conn: pg connection
    :param company: dict with company info, including id and public_key
    :param worker: instance of RequestWorker
    :param data: data about contractor
    :param skip_deleted: whether or not to skip deleted contractors (or delete them in the db.)
    :return: Action: created, updated or deleted
    """
    con_id = data.pop('id')
    deleted = data.pop('deleted')
    if deleted:
        if not skip_deleted:
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
    tag_line, ex_attrs = _get_special_extra_attr(ex_attrs, 'tag_line', 'text_short')
    primary_description, ex_attrs = _get_special_extra_attr(ex_attrs, 'primary_description', 'text_extended')
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
