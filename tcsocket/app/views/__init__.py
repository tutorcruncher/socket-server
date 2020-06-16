from aiohttp import web_exceptions
from aiohttp.web import Response
from sqlalchemy import select

from ..models import sa_con_skills, sa_contractors, sa_labels, sa_qual_levels, sa_subjects
from ..utils import json_response, slugify


async def index(request):
    return Response(text=request.app['index_html'], content_type='text/html')


ROBOTS = """\
User-agent: *
Allow: /
"""


async def robots_txt(request):
    return Response(text=ROBOTS, content_type='text/plain')


async def favicon(request):
    raise web_exceptions.HTTPMovedPermanently('https://secure.tutorcruncher.com/favicon.ico')


async def _sub_qual_list(request, q):
    q = q.where(sa_contractors.c.company == request['company'].id)
    conn = await request['conn_manager'].get_connection()
    return json_response(request, list_=[dict(s, link=f'{s.id}-{slugify(s.name)}') async for s in conn.execute(q)])


async def subject_list(request):
    q = (
        select([sa_subjects.c.id, sa_subjects.c.name, sa_subjects.c.category])
        .select_from(sa_con_skills.join(sa_contractors).join(sa_subjects))
        .order_by(sa_subjects.c.category, sa_subjects.c.id)
        .distinct(sa_subjects.c.category, sa_subjects.c.id)
    )
    return await _sub_qual_list(request, q)


async def qual_level_list(request):
    q = (
        select([sa_qual_levels.c.id, sa_qual_levels.c.name])
        .select_from(sa_con_skills.join(sa_contractors).join(sa_qual_levels))
        .order_by(sa_qual_levels.c.ranking, sa_qual_levels.c.id)
        .distinct(sa_qual_levels.c.ranking, sa_qual_levels.c.id)
    )
    return await _sub_qual_list(request, q)


async def labels_list(request):
    q = select([sa_labels.c.name, sa_labels.c.machine_name]).where(sa_labels.c.company == request['company'].id)
    conn = await request['conn_manager'].get_connection()
    return json_response(request, **{s.machine_name: s.name async for s in conn.execute(q)})
