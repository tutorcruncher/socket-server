import logging

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import and_

from ..models import sa_appointments, sa_service
from ..utils import HTTPConflictJson, json_response
from ..validation import AppointmentModel

logger = logging.getLogger('socket.views')


async def appointment_webhook(request):
    apt_id = request.match_info['id']
    appointment: AppointmentModel = request['model']

    conn = await request['conn_manager'].get_connection()
    v = await conn.execute(
        select([sa_service.c.company])
        .where(sa_service.c.id == appointment.service_id)
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
        pg_insert(sa_service)
        .values(id=appointment.service_id, company=request['company'].id, **service_insert_update)
        .on_conflict_do_update(
            index_elements=[sa_service.c.id],
            where=sa_service.c.id == appointment.service_id,
            set_=service_insert_update,
        )
    )
    apt_insert_update = appointment.dict(include={
        'appointment_topic', 'attendees_max', 'attendees_count',
        'attendees_current_ids', 'start', 'finish', 'price', 'location'
    })

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
        .where(and_(sa_appointments.c.id == apt_id, sa_service.c.company == request['company'].id))
    )
    return json_response(request, status='success' if v.rowcount else 'appointment not found')
