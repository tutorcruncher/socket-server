import logging
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

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


TC_TERMS_PATH = '/view-branch-terms/'
TC_TERMS_ROLE_TYPE = 'Client'


def add_terms_link_role_type(terms_link: Optional[str]) -> Optional[str]:
    """
    Add role_type=Client to a TutorCruncher terms link that doesn't have it.

    TutorCruncher's terms page decides which audience's terms to show from a role_type query
    parameter. Enquiry forms are Client enquiries - TutorCruncher records the ticked box as Client
    consent - so Client is the right audience. terms_link values stored before TutorCruncher split
    its terms per audience have no role_type and resolve to nothing, so the link opens an empty
    page. Adding the parameter as we serve the options means every embedded form gets working
    terms, whatever version of the frontend the page loads.

    TutorCruncher now adds role_type itself when an integration is saved, so links that already
    have one are left alone and this becomes a no-op once the stored values are all updated.
    """
    if not terms_link:
        return terms_link
    parts = urlsplit(terms_link)
    if TC_TERMS_PATH not in parts.path:
        # a link to the company's own terms page rather than TutorCruncher's - not ours to change
        return terms_link
    query = parse_qs(parts.query)
    if 'role_type' in query:
        return terms_link
    query['role_type'] = [TC_TERMS_ROLE_TYPE]
    return urlunsplit(parts._replace(query=urlencode(query, doseq=True)))


async def company_options(request):
    """
    Get a companies options
    """
    opts = CompanyOptionsModel(
        name=request['company'].name, name_display=request['company'].name_display, **(request['company'].options or {})
    )
    data = opts.dict()
    data['terms_link'] = add_terms_link_role_type(data['terms_link'])
    return json_response(request, **data)
