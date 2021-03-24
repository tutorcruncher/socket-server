import hashlib
import hmac
import json
from datetime import datetime, timedelta
from time import time

from tcsocket.app.models import sa_appointments, sa_services

from .conftest import count, create_appointment, create_company


async def test_list_appointments(cli, company, appointment):
    r = await cli.get(cli.server.app.router['appointment-list'].url_for(company='thepublickey'))
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj == {
        'results': [
            {
                'id': 456,
                'link': '456-testing-appointment',
                'topic': 'testing appointment',
                'attendees_max': 42,
                'attendees_count': 4,
                'start': '2032-01-01T12:00:00',
                'finish': '2032-01-01T13:00:00',
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
                ],
            },
        ],
        'count': 1,
    }


async def test_many_apts(cli, db_conn, company):
    await create_appointment(db_conn, company, appointment_extra={'id': 1})
    for i in range(55):
        await create_appointment(
            db_conn,
            company,
            create_service=False,
            appointment_extra=dict(
                id=i + 2,
                start=datetime(2032, 1, 1, 12, 0, 0) + timedelta(days=i + 1),
                finish=datetime(2032, 1, 1, 13, 0, 0) + timedelta(days=i + 1),
            ),
        )

    assert 56 == await count(db_conn, sa_appointments)
    assert 1 == await count(db_conn, sa_services)

    url = cli.server.app.router['appointment-list'].url_for(company='thepublickey')
    r = await cli.get(url)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['count'] == 56
    assert len(obj['results']) == 30
    assert obj['results'][0]['start'] == '2032-01-01T12:00:00'
    assert obj['results'][-1]['start'] == '2032-01-30T12:00:00'

    r = await cli.get(url.with_query({'page': '2'}))
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['count'] == 56
    assert len(obj['results']) == 26
    assert obj['results'][0]['start'] == '2032-01-31T12:00:00'
    assert obj['results'][-1]['start'] == '2032-02-25T12:00:00'

    r = await cli.get(url.with_query({'pagination': '45'}))
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert len(obj['results']) == 45

    r = await cli.get(url.with_query({'pagination': '100'}))
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert len(obj['results']) == 50


async def test_service_filter(cli, db_conn, company):
    n = datetime.utcnow()
    midnight = datetime(n.year, n.month, n.day)

    await create_appointment(db_conn, company, appointment_extra={'id': 1, 'start': midnight + timedelta(seconds=3)})
    await create_appointment(db_conn, company, appointment_extra={'id': 2}, create_service=False)
    await create_appointment(db_conn, company, appointment_extra={'id': 3}, service_extra={'id': 2})
    await create_appointment(
        db_conn, company, appointment_extra={'id': 4, 'start': midnight - timedelta(seconds=1)}, service_extra={'id': 3}
    )

    company2 = await create_company(db_conn, 'compan2_public', 'compan2_private', name='company2')
    await create_appointment(db_conn, company2, appointment_extra={'id': 5}, service_extra={'id': 4})

    url = cli.server.app.router['appointment-list'].url_for(company='thepublickey')
    r = await cli.get(url)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['count'] == 3
    assert {r['id'] for r in obj['results']} == {1, 2, 3}

    r = await cli.get(url.with_query({'service': '1'}))
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['count'] == 2
    assert {r['id'] for r in obj['results']} == {1, 2}


async def test_service_list(cli, db_conn, company):
    await create_appointment(db_conn, company, appointment_extra={'id': 1, 'start': datetime(2033, 1, 1)})
    await create_appointment(db_conn, company, appointment_extra={'id': 2}, create_service=False)
    await create_appointment(
        db_conn, company, appointment_extra={'id': 3, 'start': datetime(1986, 1, 1)}, create_service=False
    )
    await create_appointment(
        db_conn,
        company,
        appointment_extra={'id': 4, 'start': datetime(2032, 1, 1)},
        service_extra={'id': 2, 'extra_attributes': [], 'colour': '#cba'},
    )

    await create_appointment(
        db_conn, company, appointment_extra={'id': 5, 'start': datetime(1986, 1, 1)}, service_extra={'id': 3}
    )

    url = cli.server.app.router['service-list'].url_for(company='thepublickey')
    r = await cli.get(url)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj == {
        'results': [
            {'id': 2, 'name': 'testing service', 'colour': '#cba', 'extra_attributes': []},
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


def sig_sso_data(company, **kwargs):
    expires = int(time()) + 10
    data = {
        'rt': 'Client',
        'nm': 'Testing Client',
        'srs': {'3': 'Frank Foobar', '4': 'Another Student'},
        'id': 364576,
        'tz': 'Europe/London',
        'br_id': 3492,
        'br_nm': 'DinoTutors: Dino Centre',
        'exp': expires,
        'key': f'384854-{expires}-66cba424ae7783bcacfc5a75482a48c00b5e25fa',
    }
    data.update(kwargs)
    sso_data = json.dumps(data)
    return {
        'signature': hmac.new(company.private_key.encode(), sso_data.encode(), hashlib.sha1).hexdigest(),
        'sso_data': sso_data,
    }


async def test_check_client_data(cli, company, db_conn):
    await create_appointment(db_conn, company, appointment_extra={'id': 42, 'attendees_current_ids': [384924]})
    await create_appointment(
        db_conn,
        company,
        appointment_extra={'id': 43, 'attendees_current_ids': [384924, 123]},
        create_service=False,
    )
    await create_appointment(
        db_conn,
        company,
        appointment_extra={'id': 44, 'attendees_current_ids': [384924, 6, 7, 8]},
        create_service=False,
    )
    await create_appointment(
        db_conn,
        company,
        appointment_extra={'id': 45, 'attendees_current_ids': [8, 9]},
        create_service=False,
    )

    sso_args = sig_sso_data(company, srs={'384924': 'Frank Foobar', '123': 'Other Studnets'})

    url = cli.server.app.router['check-client'].url_for(company='thepublickey').with_query(sso_args)
    r = await cli.get(url)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['status'] == 'ok'
    assert obj['appointment_attendees'] == {'42': [384924], '43': [123, 384924], '44': [384924]}


async def test_submit_appointment(cli, company, appointment, other_server, worker):
    url = cli.server.app.router['book-appointment'].url_for(company='thepublickey').with_query(sig_sso_data(company))
    assert len(other_server.app['request_log']) == 0
    r = await cli.post(url, data=json.dumps({'appointment': appointment['appointment']['id'], 'student_id': '4'}))
    assert r.status == 201, await r.text()
    await worker.run_check()
    assert len(other_server.app['request_log']) == 1
    assert other_server.app['request_log'][0][0] == 'booking_post'
    assert other_server.app['request_log'][0][1]['service_recipient_id'] == 4
    assert 'service_recipient_name' not in other_server.app['request_log'][0][1]


async def test_check_ok(cli, company, appointment):
    url = cli.server.app.router['check-client'].url_for(company='thepublickey').with_query(sig_sso_data(company))
    r = await cli.get(url, data=json.dumps({'appointment': appointment['appointment']['id'], 'student_id': '4'}))
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['status'] == 'ok'


async def test_check_invalid(cli, company, appointment):
    url = (
        cli.server.app.router['check-client']
        .url_for(company='thepublickey')
        .with_query(sig_sso_data(company, rt='Contractor'))
    )
    r = await cli.get(url, data=json.dumps({'appointment': appointment['appointment']['id'], 'student_id': '4'}))
    assert r.status == 400, await r.text()
    obj = await r.json()
    assert obj['status'] == 'invalid request data'
    deets = obj['details'][0]
    assert deets['loc'] == ['rt']
    assert deets['msg'] == 'must be \"Client\"'


async def test_check_expired(cli, company, appointment):
    url = (
        cli.server.app.router['check-client'].url_for(company='thepublickey').with_query(sig_sso_data(company, exp=123))
    )
    r = await cli.get(url, data=json.dumps({'appointment': appointment['appointment']['id'], 'student_id': '4'}))
    assert r.status == 401, await r.text()
    obj = await r.json()
    assert obj == {'status': 'session expired'}


async def test_submit_appointment_student_name(cli, company, appointment, other_server, worker):
    url = cli.server.app.router['book-appointment'].url_for(company='thepublickey').with_query(sig_sso_data(company))
    assert len(other_server.app['request_log']) == 0
    r = await cli.post(
        url, data=json.dumps({'appointment': appointment['appointment']['id'], 'student_name': 'Frank Spencer'})
    )
    assert r.status == 201, await r.text()
    await worker.run_check()
    assert len(other_server.app['request_log']) == 1
    assert other_server.app['request_log'][0][0] == 'booking_post'
    assert 'service_recipient_id' not in other_server.app['request_log'][0][1]
    assert other_server.app['request_log'][0][1]['service_recipient_name'] == 'Frank Spencer'


async def test_submit_double_book(cli, company, appointment, other_server):
    url = cli.server.app.router['book-appointment'].url_for(company='thepublickey').with_query(sig_sso_data(company))
    assert len(other_server.app['request_log']) == 0
    r = await cli.post(url, data=json.dumps({'appointment': appointment['appointment']['id'], 'student_id': '3'}))
    assert r.status == 400, await r.text()
    assert {'status': 'student 3(Frank Foobar) already on appointment 456'} == await r.json()
    assert len(other_server.app['request_log']) == 0


async def test_submit_appointment_wrong_appointment(cli, company, appointment, other_server):
    url = cli.server.app.router['book-appointment'].url_for(company='thepublickey').with_query(sig_sso_data(company))
    assert len(other_server.app['request_log']) == 0
    r = await cli.post(url, data=json.dumps({'appointment': 987, 'student_id': 3}))
    assert r.status == 404, await r.text()
    assert {'status': 'appointment 987 not found'} == await r.json()
    assert len(other_server.app['request_log']) == 0


async def test_submit_appointment_no_signature(cli, company, appointment, other_server):
    url = cli.server.app.router['book-appointment'].url_for(company='thepublickey')
    assert len(other_server.app['request_log']) == 0
    r = await cli.post(url, data=json.dumps({'appointment': appointment['appointment']['id'], 'student_id': 3}))
    assert r.status == 403, await r.text()


async def test_submit_appointment_invalid_signature(cli, company, appointment, other_server):
    sig_args = sig_sso_data(company)
    sig_args['signature'] += 'x'
    url = cli.server.app.router['book-appointment'].url_for(company='thepublickey').with_query(sig_args)
    assert len(other_server.app['request_log']) == 0
    r = await cli.post(url, data=json.dumps({'appointment': appointment['appointment']['id'], 'student_id': 3}))
    assert r.status == 403, await r.text()


async def test_no_id_or_none(cli, company, appointment, other_server):
    url = cli.server.app.router['book-appointment'].url_for(company='thepublickey').with_query(sig_sso_data(company))
    assert len(other_server.app['request_log']) == 0
    r = await cli.post(url, data=json.dumps({'appointment': appointment['appointment']['id']}))
    assert r.status == 400, await r.text()
    assert 'either student_id or student_name is required' in await r.text()


async def test_slugify(cli, db_conn, company):
    await create_appointment(db_conn, company, appointment_extra={'topic': 'appointment - is - here'})

    url = cli.server.app.router['appointment-list'].url_for(company='thepublickey')
    r = await cli.get(url)
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj['results'][0]['link'] == '456-appointment-is-here'
