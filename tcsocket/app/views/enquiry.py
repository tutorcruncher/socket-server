import json
import logging
from datetime import date, datetime
from enum import Enum
from operator import itemgetter

import pydantic
from aiohttp.hdrs import METH_POST
from arq.utils import timestamp

from ..geo import get_ip
from ..utils import HTTPBadRequestJson, json_response
from ..worker import REDIS_ENQUIRY_CACHE_KEY, store_enquiry_data

logger = logging.getLogger('socket.views')
VISIBLE_FIELDS = 'client_name', 'client_email', 'client_phone', 'service_recipient_name'


async def clear_enquiry(request):
    redis = request.app['redis']
    v = await redis.delete(REDIS_ENQUIRY_CACHE_KEY % request['company'].id)
    return json_response(
        request,
        status='success',
        data_existed=bool(v)
    )


async def enquiry(request):
    company = dict(request['company'])

    redis = request.app['redis']
    raw_enquiry_options = await redis.get(REDIS_ENQUIRY_CACHE_KEY % company['id'])
    ts = timestamp()
    if raw_enquiry_options:
        enquiry_options = json.loads(raw_enquiry_options.decode())
        enquiry_last_updated = enquiry_options['last_updated']
        # 1800 so data should never expire for regularly used forms
        if (ts - enquiry_last_updated) > 1800:
            await request.app['worker'].update_enquiry_options(company)
    else:
        # no enquiry options yet exist, we have to get them now even though it will make the request slow
        enquiry_options = await request.app['worker'].get_enquiry_options(company)
        enquiry_options['last_updated'] = ts
        await store_enquiry_data(redis, company, enquiry_options)

    enq_method = enquiry_post if request.method == METH_POST else enquiry_get
    return await enq_method(request, company, enquiry_options)


FIELD_TYPE_LOOKUP = {
    'field': 'id',
    'string': 'text',
    'email': 'email',
    'choice': 'select',
    'boolean': 'checkbox',
    'integer': 'integer',
    'date': 'date',
    'datetime': 'datetime',
}
CREATE_ENUM = object()
FIELD_VALIDATION_LOOKUP = {
    'string': str,
    'email': pydantic.EmailStr,
    'choice': CREATE_ENUM,
    'boolean': bool,
    'integer': int,
    'date': date,
    'datetime': datetime,
}


class AttributeBaseModel(pydantic.BaseModel):
    @pydantic.validator('*')
    def make_serializable(cls, v):
        # datetime is a subclass of date
        if isinstance(v, date):
            return v.isoformat()
        elif isinstance(v, Enum):
            return v.value
        else:
            return v


async def enquiry_post(request, company, enquiry_options):
    data = request['model'].dict()
    data = {k: v for k, v in data.items() if v is not None}
    attributes = data.pop('attributes', None)
    referrer = request.headers.get('Referer')
    data.update(
        user_agent=request.headers.get('User-Agent'),
        ip_address=get_ip(request),
        http_referrer=referrer and referrer[:1023],
    )

    fields = {}
    for name, field_data in enquiry_options['attributes'].get('children', {}).items():
        field_type = FIELD_VALIDATION_LOOKUP[field_data['type']]
        if field_type == CREATE_ENUM:
            field_type = Enum('DynamicEnum', {f'v{i}': c['value'] for i, c in enumerate(field_data['choices'])})
        fields[name] = (field_type, ... if field_data['required'] else None)

    if fields:
        dynamic_model = pydantic.create_model('AttributeModel', **fields, __base__=AttributeBaseModel)
        try:
            attributes = dynamic_model.parse_obj(attributes or {})
        except pydantic.ValidationError as e:
            raise HTTPBadRequestJson(status='invalid attribute data', details=e.errors())
        else:
            data['attributes'] = {k: v for k, v in attributes.dict().items() if v is not None}

    await request.app['worker'].submit_enquiry(company, data)
    return json_response(request, status='enquiry submitted to TutorCruncher', status_=201)


def _convert_field(name, value, prefix=None):
    value_ = dict(value)
    ftype = FIELD_TYPE_LOOKUP[value_.pop('type')]
    if ftype is None:
        return None
    value_.pop('read_only')
    return dict(
        field=name,
        type=ftype,
        prefix=prefix,
        **value_
    )


async def enquiry_get(request, company, enquiry_options):
    # make the enquiry form data easier to render for js
    visible = filter(bool, [
        _convert_field(f, enquiry_options[f]) for f in VISIBLE_FIELDS
    ] + [
        _convert_field(k, v, 'attributes') for k, v in enquiry_options['attributes'].get('children', {}).items()
    ])

    return json_response(
        request,
        visible=sorted(visible, key=itemgetter('sort_index', )),
        hidden={'contractor': _convert_field('contractor', enquiry_options['contractor'])},
    )
