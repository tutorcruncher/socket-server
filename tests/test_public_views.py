from datetime import datetime

from tcsocket.app.models import sa_companies, sa_con_skills, sa_contractors, sa_qual_levels, sa_subjects


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
    assert r.headers.get('Access-Control-Allow-Origin') == '*'
    obj = await r.json()
    assert [
        {
            'id': 1,
            'link': '1-fred-b',
            'name': 'Fred B',
            'photo': 'http://socket.tutorcruncher.com/media/thekey/1.thumb.jpg',
            'tag_line': None,
            'url': 'http://socket.tutorcruncher.com/thekey/contractors/1',
        }
    ] == obj


async def test_get_contractor(cli, db_conn):
    v = await db_conn.execute(
        sa_companies
        .insert()
        .values(name='testing', key='thekey')
        .returning(sa_companies.c.id)
    )
    r = await v.first()
    company_id = r.id
    v = await db_conn.execute(
        sa_contractors
        .insert()
        .values(
            id=1,
            company=company_id,
            first_name='Fred',
            last_name='Bloggs',
            last_updated=datetime.now(),
            extra_attributes=[{'foo': 'bar'}]
        )
        .returning(sa_contractors.c.id)
    )
    con_id = (await v.first()).id
    v = await db_conn.execute(
        sa_subjects
        .insert()
        .values([{'name': 'Mathematics', 'category': 'Maths'}, {'name': 'Language', 'category': 'English'}])
        .returning(sa_subjects.c.id)
    )
    subjects = [r.id for r in (await v.fetchall())]

    v = await db_conn.execute(
        sa_qual_levels
        .insert()
        .values([{'name': 'GCSE', 'ranking': 16}, {'name': 'A Level', 'ranking': 18}])
        .returning(sa_qual_levels.c.id)
    )
    qual_levels = [r.id for r in (await v.fetchall())]

    await db_conn.execute(
        sa_con_skills
        .insert()
        .values([{'contractor': con_id, 'subject': s, 'qual_level': ql} for s, ql in zip(subjects, qual_levels)])
    )

    r = await cli.get(cli.server.app.router['contractor-get'].url_for(company='thekey', id=con_id, slug='x'))
    assert r.status == 200
    obj = await r.json()
    assert {
        'id': 1,
        'name': 'Fred B',
        'extra_attributes': [{'foo': 'bar'}],
        'tag_line': None,
        'photo': 'http://socket.tutorcruncher.com/media/thekey/1.jpg',
        'primary_description': None,
        'skills': [
            {
                'category': 'English',
                'qual_levels': ['A Level'],
                'subject': 'Language'
            },
            {
                'category': 'Maths',
                'qual_levels': ['GCSE'],
                'subject': 'Mathematics'
            }
        ],
    } == obj
