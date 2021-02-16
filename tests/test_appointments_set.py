from datetime import datetime, timedelta

from tcsocket.app.models import sa_appointments, sa_services
from tcsocket.app.worker import delete_old_appointments, startup

from .conftest import MockEngine, count, create_appointment, create_company, select_set, signed_request


async def create_apt(cli, company, url=None, **kwargs):
    data = dict(
        service_id=123,
        service_name='testing service',
        extra_attributes=[],
        colour='#abc',
        appointment_topic='testing appointment',
        attendees_max=42,
        attendees_count=4,
        attendees_current_ids=[1, 2, 3],
        start='1986-01-01T12:00:00',
        finish='1986-01-01T13:00:00',
        price=123.45,
        location='Whatever',
    )
    data.update(kwargs)
    return await signed_request(cli, url or f'/{company.public_key}/webhook/appointments/123', **data)


async def test_create(cli, db_conn, company):
    r = await create_apt(cli, company)
    assert r.status == 200, await r.text()

    curr = await db_conn.execute(sa_services.select())
    result = await curr.first()
    assert result.id == 123
    assert result.company == company.id
    assert result.name == 'testing service'
    assert result.colour == '#abc'
    assert result.extra_attributes == []

    curr = await db_conn.execute(sa_appointments.select())
    result = await curr.first()
    assert result.service == 123
    assert result.topic == 'testing appointment'
    assert result.attendees_max == 42
    assert result.attendees_count == 4
    assert result.attendees_current_ids == [1, 2, 3]
    assert result.start == datetime(1986, 1, 1, 12, 0)
    assert result.finish == datetime(1986, 1, 1, 13, 0)
    assert result.price == 123.45
    assert result.location == 'Whatever'


async def test_delete(cli, db_conn, company):
    url = f'/{company.public_key}/webhook/appointments/231'
    r = await create_apt(cli, company, url)
    assert r.status == 200, await r.text()

    assert 1 == await count(db_conn, sa_appointments)
    assert 1 == await count(db_conn, sa_services)

    r = await signed_request(cli, url, method_='DELETE')
    assert r.status == 200, await r.text()
    assert {'status': 'success'} == await r.json()

    assert 0 == await count(db_conn, sa_appointments)
    assert 0 == await count(db_conn, sa_services)

    # should do nothing
    r = await signed_request(cli, url, method_='DELETE')
    assert r.status == 200, await r.text()
    assert {'status': 'appointment not found'} == await r.json()

    assert 0 == await count(db_conn, sa_appointments)
    assert 0 == await count(db_conn, sa_services)


async def test_delete_keep_service(cli, db_conn, company):
    r = await create_apt(cli, company)
    assert r.status == 200, await r.text()

    url = f'/{company.public_key}/webhook/appointments/124'
    r = await create_apt(cli, company, url)
    assert r.status == 200, await r.text()

    assert 2 == await count(db_conn, sa_appointments)
    assert 1 == await count(db_conn, sa_services)

    r = await signed_request(cli, url, method_='DELETE')
    assert r.status == 200, await r.text()
    assert {'status': 'success'} == await r.json()

    assert 1 == await count(db_conn, sa_appointments)
    assert 1 == await count(db_conn, sa_services)


async def test_delete_wrong_company(cli, db_conn, company):
    company2 = await create_company(db_conn, 'compan2_public', 'compan2_private', name='company2')
    r = await create_apt(cli, company2)
    assert r.status == 200, await r.text()

    url = f'/{company.public_key}/webhook/appointments/123'
    r = await signed_request(cli, url, method_='DELETE')
    assert r.status == 200, await r.text()
    assert {'status': 'appointment not found'} == await r.json()

    assert 1 == await count(db_conn, sa_appointments)

    url = f'/{company2.public_key}/webhook/appointments/123'
    r = await signed_request(cli, url, method_='DELETE')
    assert r.status == 200, await r.text()
    assert {'status': 'success'} == await r.json()

    assert 0 == await count(db_conn, sa_appointments)


async def test_create_conflict(cli, db_conn, company):
    r = await create_apt(cli, company)
    assert r.status == 200, await r.text()

    company2 = await create_company(db_conn, 'compan2_public', 'compan2_private', name='company2')
    r = await create_apt(cli, company2)
    assert r.status == 409, await r.text()


async def test_extra_attrs(cli, db_conn, company):
    extra_attrs = [
        {'name': 'Foobar', 'type': 'checkbox', 'machine_name': 'foobar', 'value': False, 'sort_index': 124},
        {'name': 'Smash', 'type': 'text_short', 'machine_name': 'smash', 'value': 'I love to party', 'sort_index': 123},
    ]
    r = await create_apt(cli, company, extra_attributes=extra_attrs)
    assert r.status == 200, await r.text()

    curr = await db_conn.execute(sa_services.select())
    result = await curr.first()
    assert result.name == 'testing service'
    # remove sort_index and reverse so they're ordered by sort_index
    eas = list(reversed([{k: v for k, v in ea_.items() if k != 'sort_index'} for ea_ in extra_attrs]))
    assert result.extra_attributes == eas


async def test_delete_old_appointments(db_conn, company, settings):
    n = datetime.utcnow()
    await create_appointment(db_conn, company, appointment_extra={'id': 1, 'start': n}, service_extra={'id': 1})

    await create_appointment(
        db_conn, company, appointment_extra={'id': 2, 'start': n - timedelta(days=8)}, service_extra={'id': 2}
    )

    await create_appointment(
        db_conn, company, appointment_extra={'id': 3, 'start': n - timedelta(days=6)}, service_extra={'id': 3}
    )  # not old enough
    await create_appointment(
        db_conn,
        company,
        appointment_extra={'id': 4, 'start': n - timedelta(days=365)},
        service_extra={'id': 3},
        create_service=False,
    )

    ctx = {'settings': settings}
    await startup(ctx)
    ctx['pg_engine'] = MockEngine(db_conn)

    assert {(1, 1), (2, 2), (3, 3), (4, 3)} == await select_set(
        db_conn, sa_appointments.c.id, sa_appointments.c.service
    )
    assert {(1,), (2,), (3,)} == await select_set(db_conn, sa_services.c.id)

    await delete_old_appointments(ctx)

    assert {(1, 1), (3, 3)} == await select_set(db_conn, sa_appointments.c.id, sa_appointments.c.service)
    assert {(1,), (3,)} == await select_set(db_conn, sa_services.c.id)


async def test_clear_apts(cli, db_conn, company):
    await create_appointment(db_conn, company, appointment_extra={'id': 1})
    for i in range(10):
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

    assert 11 == await count(db_conn, sa_appointments)
    assert 1 == await count(db_conn, sa_services)

    url = cli.server.app.router['webhook-appointment-clear'].url_for(company='thepublickey')
    r = await signed_request(cli, url, method_='DELETE')
    assert r.status == 200
    assert {'status': 'success'} == await r.json()

    assert 0 == await count(db_conn, sa_appointments)
    assert 0 == await count(db_conn, sa_services)
