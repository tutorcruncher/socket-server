import asyncio
import json
import logging
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from operator import itemgetter

import pydantic
from aiohttp import ClientError, ClientTimeout
from aiohttp.hdrs import METH_OPTIONS, METH_POST
from aiohttp.web_response import Response
from arq.utils import timestamp_ms

from ..geo import get_ip
from ..utils import CORS_PREFLIGHT_HEADERS, HTTPBadGatewayJson, HTTPBadRequestJson, json_response
from ..validation import LinkUrl
from ..worker import REDIS_ENQUIRY_CACHE_KEY, check_grecaptcha, get_enquiry_options, post_data, store_enquiry_data

logger = logging.getLogger('socket')
VISIBLE_FIELDS = 'client_name', 'client_email', 'client_phone', 'service_recipient_name'
# The browser waits for TutorCruncher's answer, so don't make it wait for aiohttp's 5 minute default
ENQUIRY_POST_TIMEOUT = ClientTimeout(total=30)


async def clear_enquiry(request):
    redis = request.app['redis']
    v = await redis.delete(REDIS_ENQUIRY_CACHE_KEY % request['company'].id)
    return json_response(request, status='success', data_existed=bool(v))


async def enquiry(request):
    if request.method == METH_OPTIONS:
        # CORS preflight sent by browsers before a cross-origin JSON post
        return Response(status=204, headers=CORS_PREFLIGHT_HEADERS)

    company = dict(request['company'])

    redis = request.app['redis']
    raw_enquiry_options = await redis.get(REDIS_ENQUIRY_CACHE_KEY % company['id'])
    ts = timestamp_ms()
    if raw_enquiry_options:
        enquiry_options = json.loads(raw_enquiry_options)
        enquiry_last_updated = enquiry_options['last_updated']
        # 1800 so data should never expire for regularly used forms
        if (ts - enquiry_last_updated) > 1800:
            await request.app['redis'].enqueue_job('update_enquiry_options', company)
    else:
        # no enquiry options yet exist, we have to get them now even though it will make the request slow
        ctx = {'settings': request.app['settings'], 'session': request.app['session']}
        enquiry_options = await get_enquiry_options(ctx, company=company)
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
    # TutorCruncher's Stars field. TutorCruncher's own forms take whole stars too.
    'decimal': 'integer',
    'date': 'date',
    'datetime': 'datetime',
    'url': 'url',
}
CREATE_URL = object()
CREATE_ENUM = object()
CREATE_DECIMAL = object()
FIELD_VALIDATION_LOOKUP = {
    'string': str,
    'email': pydantic.EmailStr,
    'choice': CREATE_ENUM,
    'boolean': bool,
    'integer': int,
    'decimal': CREATE_DECIMAL,
    'date': date,
    'datetime': datetime,
    'url': CREATE_URL,
}


class AttributeBaseModel(pydantic.BaseModel):
    @pydantic.validator('*', pre=True)
    def empty_optional_as_absent(cls, v, field):
        # The enquiry form sends "" for a box typed in then cleared. For an optional field that is the same as never
        # filling it in, unless "" is one of its values: text, or a dropdown with an empty option.
        takes_empty = field.type_ is str or (issubclass(field.type_, Enum) and any(m.value == '' for m in field.type_))
        if v == '' and not field.required and not takes_empty:
            return None
        return v

    @pydantic.validator('*')
    def make_serializable(cls, v):
        # datetime is a subclass of date
        if isinstance(v, date):
            return v.isoformat()
        elif isinstance(v, Enum):
            return v.value
        elif isinstance(v, Decimal):
            # post_data posts the attributes with json.dumps, which can't encode a Decimal
            return float(v)
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
        if field_type == CREATE_URL:
            # TutorCruncher refuses a longer link, so refuse it here rather than lose the enquiry
            max_length = field_data.get('max_length', LinkUrl.max_length)
            field_type = type('DynamicLinkUrl', (LinkUrl,), {'max_length': max_length})
        elif field_type == CREATE_ENUM:
            field_type = Enum('DynamicEnum', {f'v{i}': c['value'] for i, c in enumerate(field_data['choices'])})
        elif field_type == CREATE_DECIMAL:
            # TutorCruncher refuses a value outside these, so refuse it here rather than lose the enquiry
            field_type = pydantic.condecimal(
                ge=field_data.get('min_value'),
                le=field_data.get('max_value'),
                max_digits=field_data.get('max_digits'),
                decimal_places=field_data.get('decimal_places'),
            )
        fields[name] = (field_type, ... if field_data['required'] else None)

    if fields:
        dynamic_model = pydantic.create_model('AttributeModel', **fields, __base__=AttributeBaseModel)
        try:
            attributes = dynamic_model.parse_obj(attributes or {})
        except pydantic.ValidationError as e:
            raise HTTPBadRequestJson(status='invalid attribute data', details=e.errors())
        else:
            data['attributes'] = {k: v for k, v in attributes.dict().items() if v is not None}

    await submit_enquiry(request, company, data)
    return json_response(request, status='enquiry submitted to TutorCruncher', status_=201)


async def submit_enquiry(request, company, data):
    """
    Send the enquiry to TutorCruncher while the browser waits, so it's told when the enquiry wasn't saved rather than
    shown a success.
    """
    settings, session = request.app['settings'], request.app['session']
    grecaptcha_response = data.pop('grecaptcha_response')
    url = settings.tc_api_root + settings.tc_enquiry_endpoint
    try:
        if not await check_grecaptcha(settings, session, company, grecaptcha_response, data['ip_address']):
            raise HTTPBadRequestJson(status='invalid reCAPTCHA')
        status, response_data = await post_data(session, url, data, company, timeout=ENQUIRY_POST_TIMEOUT)
    except (ClientError, asyncio.TimeoutError):
        logger.exception('error submitting enquiry to %s', url)
        raise HTTPBadGatewayJson(status='enquiry not submitted')

    if status == 400:
        # the form the browser has may be out of date
        await request.app['redis'].enqueue_job('update_enquiry_options', company)
        try:
            details = json.loads(response_data)
        except ValueError:
            details = None
        raise HTTPBadRequestJson(status='invalid enquiry', details=details)
    elif status not in {200, 201}:
        raise HTTPBadGatewayJson(status='enquiry not submitted')


def _convert_field(name, value, prefix=None):
    value_ = dict(value)
    ftype = FIELD_TYPE_LOOKUP[value_.pop('type')]
    if ftype is None:
        return None
    value_.pop('read_only')
    return dict(field=name, type=ftype, prefix=prefix, **value_)


async def enquiry_get(request, company, enquiry_options):
    # make the enquiry form data easier to render for js
    visible = filter(
        bool,
        [_convert_field(f, enquiry_options[f]) for f in VISIBLE_FIELDS]
        + [_convert_field(k, v, 'attributes') for k, v in enquiry_options['attributes'].get('children', {}).items()],
    )

    return json_response(
        request,
        visible=sorted(
            visible,
            key=itemgetter(
                'sort_index',
            ),
        ),
        hidden={'contractor': _convert_field('contractor', enquiry_options['contractor'])},
    )
