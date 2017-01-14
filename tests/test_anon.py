from datetime import datetime

from app.models import sa_companies, sa_contractors


async def test_index(cli):
    r = await cli.get('/')
    assert r.status == 200
    # TODO test content when we have some


async def test_list_contractors(cli, db_conn):
    v = await db_conn.execute(
        sa_companies
        .insert()
        .values(name='testing', key='thekey')
        .returning(sa_companies.c.id)
    )
    r = await v.first()
    company_id = r.id
    await db_conn.execute(
        sa_contractors
        .insert()
        .values(id=1, company=company_id, first_name='Fred', last_name='Bloggs', last_updated=datetime.now())
    )
    r = await cli.get(cli.server.app.router['contractor-list'].url_for(company='thekey'))
    assert r.status == 200
    obj = await r.json()
    assert [
        {'id': 1, 'name': 'Fred B', 'photo': None, 'slug': 'fred-b', 'tag_line': None}
    ] == obj
