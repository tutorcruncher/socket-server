import hashlib
import hmac
import logging
from datetime import datetime, timezone
from operator import attrgetter
from secrets import compare_digest
from typing import Dict

from pydantic import BaseModel, Protocol, ValidationError, validator
from sqlalchemy import distinct, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import and_
from sqlalchemy.sql import functions as sql_f

from ..models import sa_appointments, sa_services
from ..utils import (HTTPBadRequestJson, HTTPConflictJson, HTTPForbiddenJson, HTTPNotFoundJson, HTTPUnauthorizedJson,
                     get_arg, get_pagination, json_response, slugify)
from ..validation import AppointmentModel, BookingModel

logger = logging.getLogger('socket.views')
apt_c = sa_appointments.c
ser_c = sa_services.c


async def appointment_webhook(request):
    apt_id = request.match_info['id']
    appointment: AppointmentModel = request['model']

    conn = await request['conn_manager'].get_connection()
    v = await conn.execute(
        select([ser_c.company])
        .where(ser_c.id == appointment.service_id)
    )
    r = await v.first()
    if r and r.company != request['company'].id:
        raise HTTPConflictJson(
            status='service conflict',
            details=f'service {appointment.service_id} already exists and is associated with another company',
        )

    service_insert_update = dict(
        name=appointment.service_name,
        colour=appointment.colour,
        extra_attributes=[ea.dict(exclude={'sort_index'})
                          for ea in sorted(appointment.extra_attributes, key=attrgetter('sort_index'))],
    )

    await conn.execute(
        pg_insert(sa_services)
        .values(id=appointment.service_id, company=request['company'].id, **service_insert_update)
        .on_conflict_do_update(
            index_elements=[ser_c.id],
            where=ser_c.id == appointment.service_id,
            set_=service_insert_update,
        )
    )
    apt_insert_update = appointment.dict(include={
        'attendees_max', 'attendees_count', 'attendees_current_ids', 'start', 'finish', 'price', 'location'
    })
    apt_insert_update['topic'] = appointment.appointment_topic

    await conn.execute(
        pg_insert(sa_appointments)
        .values(id=apt_id, service=appointment.service_id, **apt_insert_update)
        .on_conflict_do_update(
            index_elements=[apt_c.id],
            where=apt_c.id == apt_id,
            set_=apt_insert_update,
        )
    )
    return json_response(request, status='success')


async def appointment_webhook_delete(request):
    apt_id = request.match_info['id']
    conn = await request['conn_manager'].get_connection()
    v = await conn.execute(
        sa_appointments.delete()
        .where(and_(apt_c.id == apt_id, ser_c.company == request['company'].id))
    )
    return json_response(request, status='success' if v.rowcount else 'appointment not found')


APT_LIST_FIELDS = (
    apt_c.id, apt_c.topic, apt_c.attendees_max, apt_c.attendees_count, apt_c.start, apt_c.finish,
    apt_c.price, apt_c.location,
    ser_c.id, ser_c.name, ser_c.colour, ser_c.extra_attributes
)


def _today():
    now = datetime.utcnow()
    return datetime(now.year, now.month, now.day, 0, 0)


async def appointment_list(request):
    company = request['company']
    pagination, offset = get_pagination(request)

    where = ser_c.company == company.id, apt_c.start > _today()
    service_id = get_arg(request, 'service')
    if service_id:
        where += apt_c.service == service_id,

    conn = await request['conn_manager'].get_connection()
    results = [dict(
        id=row.appointments_id,
        link=f'{row.appointments_id}-{slugify(row.appointments_topic)}',
        topic=row.appointments_topic,
        attendees_max=row.appointments_attendees_max,
        attendees_count=row.appointments_attendees_count,
        start=row.appointments_start.isoformat(),
        finish=row.appointments_finish.isoformat(),
        price=row.appointments_price,
        location=row.appointments_location,
        service_id=row.services_id,
        service_name=row.services_name,
        service_colour=row.services_colour,
        service_extra_attributes=row.services_extra_attributes,
    ) async for row in conn.execute(
        select(APT_LIST_FIELDS, use_labels=True)
        .select_from(sa_appointments.join(sa_services))
        .where(and_(*where))
        .order_by(apt_c.start)
        .offset(offset)
        .limit(pagination)
    )]

    q_count = select([sql_f.count()]).select_from(sa_appointments.join(sa_services)).where(and_(*where))
    cur_count = await conn.execute(q_count)

    return json_response(
        request,
        results=results,
        count=(await cur_count.first())[0],
    )


async def service_list(request):
    company = request['company']
    pagination, offset = get_pagination(request)

    where = ser_c.company == company.id, apt_c.start > _today()
    q1 = (
        select([ser_c.id, ser_c.name, ser_c.colour, ser_c.extra_attributes, sql_f.min(apt_c.start).label('min_start')])
        .select_from(sa_appointments.join(sa_services))
        .where(and_(*where))
        .group_by(ser_c.id)
        .alias('q1')
    )

    conn = await request['conn_manager'].get_connection()
    results = [dict(row) async for row in conn.execute(
        select([q1.c.id, q1.c.name, q1.c.colour, q1.c.extra_attributes])
        .select_from(q1)
        .order_by(q1.c.min_start)
        .offset(offset)
        .limit(pagination)
    )]

    cur_count = await conn.execute(
        select([sql_f.count(distinct(ser_c.id))])
        .select_from(sa_appointments.join(sa_services))
        .where(and_(*where))
    )

    return json_response(
        request,
        results=results,
        count=(await cur_count.first())[0],
    )


class SSOData(BaseModel):
    role_type: str
    name: str
    students: Dict[int, str]
    expires: datetime
    key: str

    @validator('role_type')
    def check_role_type(cls, v):
        if v != 'Client':
            raise ValueError('must be "Client"')

    class Config:
        fields = {
            'role_type': 'rt',
            'name': 'nm',
            'students': 'srs',
            'expires': 'exp',
        }


def _get_sso_data(request, company) -> SSOData:
    sso_data_ = request.query.get('sso_data', '-')
    expected_sig = hmac.new(company.private_key.encode(), sso_data_.encode(), hashlib.sha1).hexdigest()
    if not compare_digest(expected_sig, request.query.get('signature', '-')):
        raise HTTPForbiddenJson(status='invalid signature')
    try:
        sso_data: SSOData = SSOData.parse_raw(sso_data_, proto=Protocol.json)
    except ValidationError as e:
        raise HTTPBadRequestJson(
            status='invalid request data',
            details=e.errors_dict,
        )
    else:
        if sso_data.expires < datetime.astimezone(datetime.now(), timezone.utc):
            raise HTTPUnauthorizedJson(status='session expired')
        return sso_data


async def check_client(request):
    company = request['company']
    sso_data = _get_sso_data(request, company)

    q = (
        select([apt_c.id, apt_c.attendees_current_ids])
        .select_from(sa_appointments.join(sa_services))
        .where(and_(
            ser_c.company == company.id,
            apt_c.start > datetime.utcnow(),
            apt_c.attendees_current_ids.overlap(list(sso_data.students.keys()))
        ))
        .limit(100)
    )
    conn = await request['conn_manager'].get_connection()
    return json_response(
        request,
        status='ok',
        appointment_attendees={
            r.id: sorted(set(r.attendees_current_ids) & sso_data.students.keys())
            async for r in conn.execute(q)
        }
    )


async def book_appointment(request):
    company = request['company']
    sso_data = _get_sso_data(request, company)

    booking: BookingModel = request['model']
    if booking.student_id and booking.student_id not in sso_data.students:
        raise HTTPBadRequestJson(status=f'student {booking.student_id} not associated with this client')

    conn = await request['conn_manager'].get_connection()
    v = await conn.execute(
        select([apt_c.attendees_current_ids])
        .select_from(sa_appointments.join(sa_services))
        .where(and_(
            ser_c.company == company.id,
            apt_c.start > datetime.utcnow(),
            apt_c.id == booking.appointment,
        ))
    )
    r = await v.first()
    if not r:
        raise HTTPNotFoundJson(status=f'appointment {booking.appointment} not found')

    if booking.student_id:
        apt_attendees = set(r.attendees_current_ids)
        if booking.student_id in apt_attendees:
            raise HTTPBadRequestJson(status=f'student {booking.student_id}({sso_data.students[booking.student_id]}) '
                                            f'already on appointment {booking.appointment}')

    data = {
        'client_key': sso_data.key,
        'service_recipient_id': booking.student_id,
        'service_recipient_name': booking.student_name or None,
        'appointment': booking.appointment,
    }
    data = {k: v for k, v in data.items() if v is not None}
    await request.app['worker'].submit_booking(dict(company), data)
    return json_response(request, status='ok', status_=201)
