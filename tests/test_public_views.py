import json
from datetime import datetime

from tcsocket.app.models import sa_companies, sa_con_skills, sa_contractors, sa_qual_levels, sa_subjects


async def test_index(cli):
    r = await cli.get('/')
    assert r.status == 200
    assert "You're looking at TutorCruncher socket's API" in await r.text()


async def test_list_contractors(cli, db_conn):
    v = await db_conn.execute(
        sa_companies
        .insert()
        .values(name='testing', public_key='thepublickey', private_key='theprivatekey')
        .returning(sa_companies.c.id)
    )
    r = await v.first()
    company_id = r.id
    await db_conn.execute(
        sa_contractors
        .insert()
        .values(id=1, company=company_id, first_name='Fred', last_name='Bloggs', last_updated=datetime.now())
    )
    r = await cli.get(cli.server.app.router['contractor-list'].url_for(company='thepublickey'))
    assert r.status == 200
    assert r.headers.get('Access-Control-Allow-Origin') == '*'
    obj = await r.json()
    assert [
        {
            'id': 1,
            'link': '1-fred-b',
            'name': 'Fred B',
            'photo': 'https://socket.tutorcruncher.com/media/thepublickey/1.thumb.jpg',
            'tag_line': None,
            'primary_description': None,
            'town': None,
            'country': None,
            'url': 'https://socket.tutorcruncher.com/thepublickey/contractors/1',
        }
    ] == obj


async def test_get_contractor(cli, db_conn):
    v = await db_conn.execute(
        sa_companies
        .insert()
        .values(name='testing', public_key='thepublickey', private_key='theprivatekey')
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

    r = await cli.get(cli.server.app.router['contractor-get'].url_for(company='thepublickey', id=con_id, slug='x'))
    assert r.status == 200
    obj = await r.json()
    assert {
        'id': 1,
        'name': 'Fred B',
        'town': None,
        'country': None,
        'extra_attributes': [{'foo': 'bar'}],
        'tag_line': None,
        'photo': 'https://socket.tutorcruncher.com/media/thepublickey/1.jpg',
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


async def test_missing_url(cli):
    r = await cli.get('/foobar')
    assert r.status == 404, await r.text()


async def test_get_enquiry(cli, company, other_server):
    r = await cli.get(cli.server.app.router['enquiry'].url_for(company=company.public_key))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert len(data) == 4
    assert data['visible'][0]['field'] == 'client_name'
    assert data['visible'][0]['max_length'] == 255
    assert data['last_updated'] == 0
    # once to get immediate response, once "on the worker"
    assert other_server.app['request_log'] == ['enquiry_options', 'enquiry_options']

    r = await cli.get(cli.server.app.router['enquiry'].url_for(company=company.public_key))
    assert r.status == 200, await r.text()
    data = await r.json()
    assert len(data) == 4
    assert 1e9 < data['last_updated'] < 2e9
    # no more requests as data came from cache
    assert other_server.app['request_log'] == ['enquiry_options', 'enquiry_options']


async def test_post_enquiry(cli, company, other_server):
    data = {
        'client_name': 'Cat Flap',
        'client_phone': '123',
    }
    url = cli.server.app.router['enquiry'].url_for(company=company.public_key)
    r = await cli.post(url, data=json.dumps(data), headers={'User-Agent': 'Testing Browser'})
    assert r.status == 201, await r.text()
    data = await r.json()
    assert data == {'status': 'enquiry submitted to TutorCruncher'}
    assert other_server.app['request_log'] == [
        ('enquiry_post', {
            'client_name': 'Cat Flap',
            'client_phone': '123',
            'user_agent': 'Testing Browser',
            'ip_address': None,
            'http_referrer': None}
         )
    ]
