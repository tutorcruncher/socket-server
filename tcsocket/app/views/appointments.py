import logging

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import and_
from sqlalchemy.sql.functions import count as count_func

from ..models import sa_appointments, sa_services
from ..utils import HTTPConflictJson, get_arg, json_response, slugify
from ..validation import AppointmentModel

logger = logging.getLogger('socket.views')


async def appointment_webhook(request):
    apt_id = request.match_info['id']
    appointment: AppointmentModel = request['model']

    conn = await request['conn_manager'].get_connection()
    v = await conn.execute(
        select([sa_services.c.company])
        .where(sa_services.c.id == appointment.service_id)
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
        extra_attributes=[ea.dict() for ea in appointment.extra_attributes],
    )

    await conn.execute(
        pg_insert(sa_services)
        .values(id=appointment.service_id, company=request['company'].id, **service_insert_update)
        .on_conflict_do_update(
            index_elements=[sa_services.c.id],
            where=sa_services.c.id == appointment.service_id,
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
            index_elements=[sa_appointments.c.id],
            where=sa_appointments.c.id == apt_id,
            set_=apt_insert_update,
        )
    )
    return json_response(request, status='success')


async def appointment_webhook_delete(request):
    apt_id = request.match_info['id']
    conn = await request['conn_manager'].get_connection()
    v = await conn.execute(
        sa_appointments.delete()
        .where(and_(sa_appointments.c.id == apt_id, sa_services.c.company == request['company'].id))
    )
    return json_response(request, status='success' if v.rowcount else 'appointment not found')


async def appointment_list(request):
    company = request['company']
    where = sa_services.c.company == company.id,
    # TODO: service filter

    page = get_arg(request, 'page', default=1)
    pagination = min(get_arg(request, 'pagination', default=30), 50)
    offset = (page - 1) * pagination
    c = sa_appointments.c
    fields = (
        c.id, c.service, c.topic, c.attendees_max, c.attendees_count, c.attendees_current_ids, c.start, c.finish,
        c.price, c.location, sa_services.c.name, sa_services.c.colour
    )
    q_iter = (
        select(fields)
        .select_from(sa_appointments.join(sa_services))
        .where(and_(*where))
        .order_by(sa_appointments.c.start)
        .offset(offset)
        .limit(pagination)
    )
    conn = await request['conn_manager'].get_connection()
    results = []
    async for row in conn.execute(q_iter):
        results.append(dict(
            # url=route_url(request, 'appointment-get', company=company.public_key, id=row.id),
            link='{}-{}'.format(row.id, slugify(row.topic)),
            topic=row.topic,
            attendees_max=row.attendees_max,
            attendees_count=row.attendees_count,
            attendees_current_ids=row.attendees_current_ids,
            start=row.start.isoformat(),
            finish=row.finish.isoformat(),
            price=row.price,
            location=row.location,
        ))

    q_count = select([count_func(sa_appointments.c.id)]).where(and_(*where))
    cur_count = await conn.execute(q_count)

    return json_response(
        request,
        results=results,
        count=(await cur_count.first())[0],
    )
