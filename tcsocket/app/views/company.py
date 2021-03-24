import logging

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..models import sa_companies
from ..utils import HTTPConflictJson, json_response
from ..validation import CompanyCreateModal, CompanyOptionsModel, CompanyUpdateModel

logger = logging.getLogger('socket')


async def company_create(request):
    """
    Create a new company.

    Authentication and json parsing are done by middleware.
    """
    data = await request.json()
    update_contractors = data.pop('update_contractors', True)
    company: CompanyCreateModal = request['model']
    existing_company = bool(company.private_key)
    data = company.dict()

    conn = await request['conn_manager'].get_connection()
    v = await conn.execute(
        pg_insert(sa_companies)
        .values(**data)
        .on_conflict_do_nothing()
        .returning(sa_companies.c.id, sa_companies.c.public_key, sa_companies.c.private_key, sa_companies.c.name)
    )
    new_company = await v.first()
    if new_company is None:
        raise HTTPConflictJson(
            status='duplicate',
            details='the supplied data conflicts with an existing company',
        )
    else:
        logger.info(
            'created company "%s", id %d, public key %s, private key %s',
            new_company.name,
            new_company.id,
            new_company.public_key,
            new_company.private_key,
        )
        if update_contractors and existing_company:
            await request.app['redis'].enqueue_job('update_contractors', company=dict(new_company))
        return json_response(
            request,
            status_=201,
            status='success',
            details={
                'name': new_company.name,
                'public_key': new_company.public_key,
                'private_key': new_company.private_key,
            },
        )


OPTIONS_FIELDS = {
    'show_stars',
    'display_mode',
    'router_mode',
    'show_hours_reviewed',
    'show_labels',
    'show_location_search',
    'show_subject_filter',
    'terms_link',
    'sort_on',
    'pagination',
    'auth_url',
    'distance_units',
}


async def company_update(request):
    """
    Modify a company.
    """
    data = await request.json()
    update_contractors = data.pop('update_contractors', True)
    company: CompanyUpdateModel = request['model']
    data = company.dict(include={'name', 'public_key', 'private_key', 'name_display'})
    data = {k: v for k, v in data.items() if v is not None}
    if company.domains != 'UNCHANGED':
        data['domains'] = company.domains

    options = company.dict(include=OPTIONS_FIELDS)
    options = {k: v for k, v in options.items() if v is not None}
    if company.currency:
        options['currency'] = company.currency.dict()
    if options:
        data['options'] = options

    conn = await request['conn_manager'].get_connection()
    public_key = request['company'].public_key
    c = sa_companies.c
    if data:
        await conn.execute(update(sa_companies).values(**data).where(c.public_key == public_key))
        logger.info('company "%s" updated, %s', public_key, data)

    select_fields = c.id, c.public_key, c.private_key, c.name_display, c.domains
    q = select(select_fields).where(c.public_key == public_key)
    result = await conn.execute(q)
    company: dict = dict(await result.first())

    if update_contractors:
        await request.app['redis'].enqueue_job('update_contractors', company=company)
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
    opts = CompanyOptionsModel(
        name=request['company'].name, name_display=request['company'].name_display, **(request['company'].options or {})
    )
    return json_response(request, **opts.dict())
