from typing import List

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import and_

from .logs import logger
from .models import Action, sa_con_skills, sa_contractors, sa_labels, sa_qual_levels, sa_subjects
from .utils import HTTPForbiddenJson, HTTPNotFoundJson
from .validation import ContractorModel


def _distinct(iter, key):
    sofar = set()
    for item in iter:
        v = getattr(item, key)
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
                {'id': s.subject_id, 'name': s.subject, 'category': s.category}
                for s in _distinct(skills, 'subject_id')
            ])
            .on_conflict_do_nothing()
        )
        await conn.execute(
            pg_insert(sa_qual_levels)
            .values([
                {'id': s.qual_level_id, 'name': s.qual_level, 'ranking': s.qual_level_ranking}
                for s in _distinct(skills, 'qual_level_id')
            ])
            .on_conflict_do_nothing()
        )

        con_skills_to_create = {(s.subject_id, s.qual_level_id) for s in skills}

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


async def _set_labels(conn, company_id, labels):
    """
    create missing labels, then create contractor labels for them.
    """
    if not labels:
        return
    async with conn.begin():
        stmt = pg_insert(sa_labels).values([
            {'company': company_id, 'machine_name': l.machine_name, 'name': l.name}
            for l in labels
        ])
        await conn.execute(
            stmt
            .on_conflict_do_update(
                index_elements=['company', 'machine_name'],
                set_=dict(name=stmt.excluded.name)
            )
        )


def _get_special_extra_attr(extra_attributes: List[ContractorModel.ExtraAttributeModel], machine_name, attr_type):
    """
    Find special extra attributes suitable for tag_line and primary_description.
    """
    eas = [ea for ea in extra_attributes if ea.type == attr_type]
    if eas:
        eas.sort(key=lambda ea: (ea.machine_name != machine_name, ea.sort_index))
        ea = eas[0]
        return ea.value, [ea_ for ea_ in extra_attributes if ea_.id != ea.id]
    else:
        return None, extra_attributes


async def contractor_set(*, conn, company, worker, contractor: ContractorModel, skip_deleted=False) -> Action:
    """
    Create or update a contractor.

    :param conn: pg connection
    :param company: dict with company info, including id and public_key
    :param worker: instance of RequestWorker
    :param contractor: data about contractor
    :param skip_deleted: whether or not to skip deleted contractors (or delete them in the db.)
    :return: Action: created, updated or deleted
    """
    if contractor.deleted:
        if not skip_deleted:
            curr = await conn.execute(
                sa_contractors
                .delete()
                .where(and_(sa_contractors.c.company == company['id'], sa_contractors.c.id == contractor.id))
                .returning(sa_contractors.c.id)
            )
            if not await curr.first():
                raise HTTPNotFoundJson(
                    status='not found',
                    details=f'contractor with id {contractor.id} not found',
                )
        return Action.deleted

    data = dict(
        first_name=contractor.first_name,
        last_name=contractor.last_name,
        town=contractor.town,
        country=contractor.country,
        last_updated=contractor.last_updated,
        labels=[l.machine_name for l in contractor.labels],
    )
    if contractor.location:
        data.update(contractor.location.dict())

    for f in ('review_rating', 'review_apt_duration'):
        v = getattr(contractor, f)
        if v is not None:
            data[f] = v

    ex_attrs = [ea for ea in contractor.extra_attributes if ea.value is not None]
    tag_line, ex_attrs = _get_special_extra_attr(ex_attrs, 'tag_line', 'text_short')
    primary_description, ex_attrs = _get_special_extra_attr(ex_attrs, 'primary_description', 'text_extended')
    data.update(
        extra_attributes=[ea_.dict() for ea_ in ex_attrs],
        tag_line=tag_line,
        primary_description=primary_description,
    )
    v = await conn.execute(
        pg_insert(sa_contractors)
        .values(id=contractor.id, company=company['id'], action=Action.created, **data)
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
            details=f'you do not have permission to update contractor {contractor.id}',
        )
    await _set_skills(conn, contractor.id, contractor.skills)
    await _set_labels(conn, company['id'], contractor.labels)
    contractor.photo and await worker.get_image(company['public_key'], contractor.id, contractor.photo)
    logger.info('%s contractor on %s', r.action, company['public_key'])
    return r.action
