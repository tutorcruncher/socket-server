from datetime import datetime

from tcsocket.app.models import sa_appointments, sa_service

from .conftest import count, signed_post


async def create_api(cli, company, url=None, **kwargs):
    data = dict(
        service_id=123,
        service_name='testing service',
        extra_attributes=[],
        colour='#abc',
        appointment_topic='testing appointment',
        attendees_max=42,
        attendees_count=4,
        attendees_current_ids=[1, 2, 3],
        start='2032-01-01T12:00:00',
        finish='2032-01-01T13:00:00',
        price=123.45,
        location='Whatever',
    )
    data.update(kwargs)
    return await signed_post(
        cli,
        url or f'/{company.public_key}/webhook/appointments/123',
        **data
    )


async def test_create(cli, db_conn, company):
    r = await create_api(cli, company)
    assert r.status == 200, await r.text()

    curr = await db_conn.execute(sa_service.select())
    result = await curr.first()
    assert result.id == 123
    assert result.company == company.id
    assert result.name == 'testing service'
    assert result.colour == '#abc'
    assert result.extra_attributes == []

    curr = await db_conn.execute(sa_appointments.select())
    result = await curr.first()
    assert result.service == 123
    assert result.appointment_topic == 'testing appointment'
    assert result.attendees_max == 42
    assert result.attendees_count == 4
    assert result.attendees_current_ids == [1, 2, 3]
    assert result.start == datetime(2032, 1, 1, 12, 0)
    assert result.finish == datetime(2032, 1, 1, 13, 0)
    assert result.price == 123.45
    assert result.location == 'Whatever'


async def test_delete(cli, db_conn, company):
    url = f'/{company.public_key}/webhook/appointments/231'
    r = await create_api(cli, company, url)
    assert r.status == 200, await r.text()

    assert 1 == await count(db_conn, sa_appointments)
    assert 1 == await count(db_conn, sa_service)

    r = await signed_post(cli, url, method_='DELETE')
    assert r.status == 200, await r.text()
    assert {'status': 'success', 'service_deleted': True} == await r.json()

    assert 0 == await count(db_conn, sa_appointments)
    assert 0 == await count(db_conn, sa_service)

    # should do nothing
    r = await signed_post(cli, url, method_='DELETE')
    assert r.status == 404, await r.text()

    assert 0 == await count(db_conn, sa_appointments)
    assert 0 == await count(db_conn, sa_service)


async def test_delete_keep_service(cli, db_conn, company):
    r = await create_api(cli, company)
    assert r.status == 200, await r.text()

    url = f'/{company.public_key}/webhook/appointments/124'
    r = await create_api(cli, company, url)
    assert r.status == 200, await r.text()

    assert 2 == await count(db_conn, sa_appointments)
    assert 1 == await count(db_conn, sa_service)

    r = await signed_post(cli, url, method_='DELETE')
    assert r.status == 200, await r.text()
    assert {'status': 'success', 'service_deleted': False} == await r.json()

    assert 1 == await count(db_conn, sa_appointments)
    assert 1 == await count(db_conn, sa_service)
