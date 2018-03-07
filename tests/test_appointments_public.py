from datetime import datetime, timedelta

from tcsocket.app.models import sa_appointments, sa_services

from .conftest import count, create_appointment, create_company


async def test_list_appointments(cli, company, appointment):
    r = await cli.get(cli.server.app.router['appointment-list'].url_for(company='thepublickey'))
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj == {
        'results': [
            {
                'link': '456-testing-appointment',
                'topic': 'testing appointment',
                'attendees_max': 42,
                'attendees_count': 4,
                'start': '1986-01-01T12:00:00',
                'finish': '1986-01-01T13:00:00',
                'price': 123.45,
                'location': 'Whatever',
                'service_id': 1,
                'service_name': 'testing service',
                'service_colour': '#abc',
                'service_extra_attributes': [
                    {
                        'name': 'Foobar',
                        'type': 'text_short',
                        'machine_name': 'foobar',
                        'value': 'this is the value of foobar',
                    }
                ]
            },
        ],
        'count': 1,
    }


async def test_many_apts(cli, db_conn, company):
    await create_appointment(db_conn, company, appointment_extra={'id': 1})
    for i in range(55):
        await create_appointment(db_conn, company, create_service=False, appointment_extra=dict(
            id=i + 2,
            start=datetime(1986, 1, 1, 12, 0, 0) + timedelta(days=i + 1),
            finish=datetime(1986, 1, 1, 13, 0, 0) + timedelta(days=i + 1),
        ))

    assert 56 == await count(db_conn, sa_appointments)
    assert 1 == await count(db_conn, sa_services)

    url = cli.server.app.router['appointment-list'].url_for(company='thepublickey')
    r = await cli.get(url)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['count'] == 56
    assert len(obj['results']) == 30
    assert obj['results'][0]['start'] == '1986-01-01T12:00:00'
    assert obj['results'][-1]['start'] == '1986-01-30T12:00:00'

    r = await cli.get(url.with_query({'page': '2'}))
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['count'] == 56
    assert len(obj['results']) == 26
    assert obj['results'][0]['start'] == '1986-01-31T12:00:00'
    assert obj['results'][-1]['start'] == '1986-02-25T12:00:00'

    r = await cli.get(url.with_query({'pagination': '45'}))
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert len(obj['results']) == 45

    r = await cli.get(url.with_query({'pagination': '100'}))
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert len(obj['results']) == 50


async def test_service_filter(cli, db_conn, company):
    await create_appointment(db_conn, company, appointment_extra={'id': 1})
    await create_appointment(db_conn, company, appointment_extra={'id': 2}, create_service=False)
    await create_appointment(db_conn, company, appointment_extra={'id': 3}, service_extra={'id': 2})
    await create_appointment(db_conn, company, appointment_extra={'id': 4, 'start': datetime(2032, 1, 1)},
                             service_extra={'id': 3})

    company2 = await create_company(db_conn, 'compan2_public', 'compan2_private', name='company2')
    await create_appointment(db_conn, company2, appointment_extra={'id': 5}, service_extra={'id': 4})

    url = cli.server.app.router['appointment-list'].url_for(company='thepublickey')
    r = await cli.get(url)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['count'] == 3
    assert {int(r['link'].split('-', 1)[0]) for r in obj['results']} == {1, 2, 3}

    r = await cli.get(url.with_query({'service': '1'}))
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['count'] == 2
    assert {int(r['link'].split('-', 1)[0]) for r in obj['results']} == {1, 2}


async def test_service_list(cli, db_conn, company):
    await create_appointment(db_conn, company, appointment_extra={'id': 1, 'start': datetime(1987, 1, 1)})
    await create_appointment(db_conn, company, appointment_extra={'id': 2}, create_service=False)
    await create_appointment(db_conn, company, appointment_extra={'id': 3, 'start': datetime(2032, 1, 1)},
                             create_service=False)
    await create_appointment(db_conn, company, appointment_extra={'id': 4, 'start': datetime(1986, 1, 1)},
                             service_extra={'id': 2, 'extra_attributes': [], 'colour': '#cba'})

    await create_appointment(db_conn, company, appointment_extra={'id': 5, 'start': datetime(2032, 1, 1)},
                             service_extra={'id': 3})

    url = cli.server.app.router['service-list'].url_for(company='thepublickey')
    r = await cli.get(url)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj == {
        'results': [
            {
                'id': 2,
                'name': 'testing service',
                'colour': '#cba',
                'extra_attributes': [],
            },
            {
                'id': 1,
                'name': 'testing service',
                'colour': '#abc',
                'extra_attributes': [
                    {
                        'name': 'Foobar',
                        'type': 'text_short',
                        'value': 'this is the value of foobar',
                        'machine_name': 'foobar',
                    },
                ],
            },
        ],
        'count': 2,
    }
