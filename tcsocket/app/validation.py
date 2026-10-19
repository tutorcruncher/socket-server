import ipaddress
import re
from datetime import datetime
from enum import Enum, unique
from secrets import token_hex
from typing import Any, List, Optional
from urllib.parse import urlsplit

from pydantic import BaseModel, EmailStr, NoneStr, constr, validator
from pydantic.errors import AnyStrMaxLengthError, PydanticValueError
from pydantic.validators import str_validator

EXTRA_ATTR_TYPES = 'checkbox', 'text_short', 'text_extended', 'integer', 'stars', 'dropdown', 'datetime', 'date'

MISSING = object()

# TutorCruncher checks a Link with Django's URLValidator(schemes=['http', 'https']). These are its patterns, so socket
# refuses every link TutorCruncher would refuse, rather than accept it and lose the enquiry.
_UL = chr(0xA1) + '-' + chr(0xFFFF)  # unicode letters
_IPV4 = (
    r'(?:0|25[0-5]|2[0-4][0-9]|1[0-9]?[0-9]?|[1-9][0-9]?)(?:\.(?:0|25[0-5]|2[0-4][0-9]|1[0-9]?[0-9]?|[1-9][0-9]?)){3}'
)
_HOST = (
    r'(?:[a-z' + _UL + r'0-9](?:[a-z' + _UL + r'0-9-]{0,61}[a-z' + _UL + r'0-9])?'  # hostname
    r'(?:\.(?!-)[a-z' + _UL + r'0-9-]{1,63}(?<!-))*'  # domain
    r'\.(?!-)(?:[a-z' + _UL + r'-]{2,63}|xn--[a-z0-9]{1,59})(?<!-)\.?'  # top level domain
    r'|localhost)'
)
LINK_URL_RE = re.compile(
    r'^(?:[a-z0-9.+-]*)://'  # scheme, checked separately
    r'(?:[^\s:@/]+(?::[^\s:@/]*)?@)?'  # user:pass
    r'(?:' + _IPV4 + r'|\[[0-9a-f:.]+\]|' + _HOST + r')'
    r'(?::[0-9]{1,5})?'  # port
    r'(?:[/?#][^\s]*)?'  # path
    r'\Z',
    re.IGNORECASE,
)


def is_link_url(v: str) -> bool:
    if {'\t', '\r', '\n'} & set(v) or v.split('://')[0].lower() not in ('http', 'https'):
        return False
    try:
        split = urlsplit(v)
    except ValueError:
        return False
    if not LINK_URL_RE.search(v):
        return False
    ipv6 = re.search(r'^\[(.+)\](?::[0-9]{1,5})?$', split.netloc)
    if ipv6:
        try:
            if len(ipv6[1]) > 39:
                return False
            ipaddress.IPv6Address(ipv6[1])
        except ValueError:
            return False
    return split.hostname is not None and len(split.hostname) <= 253


class LinkUrlError(PydanticValueError):
    code = 'url'
    msg_template = 'Enter a valid URL.'


class LinkUrl(str):
    """
    A Link custom field's web address, checked as TutorCruncher checks it: spaces at either end removed, then
    is_link_url. It is passed on as typed.
    """

    max_length = 2048  # Django's MAX_URL_LENGTH, which TutorCruncher's validator uses

    @classmethod
    def __get_validators__(cls):
        yield str_validator
        yield cls.validate

    @classmethod
    def validate(cls, v: str) -> str:
        v = v.strip()
        if len(v) > cls.max_length:
            raise AnyStrMaxLengthError(limit_value=cls.max_length)
        if not is_link_url(v):
            raise LinkUrlError()
        return v


@unique
class NameOptions(str, Enum):
    first_name = 'first_name'
    first_name_initial = 'first_name_initial'
    full_name = 'full_name'


@unique
class DisplayMode(str, Enum):
    grid = 'grid'
    list = 'list'
    enquiry = 'enquiry'
    enquiry_modal = 'enquiry-modal'


@unique
class RouterMode(str, Enum):
    hash = 'hash'
    history = 'history'


@unique
class SortOn(str, Enum):
    name = 'name'
    review_rating = 'review_rating'
    last_updated = 'last_updated'


class CompanyCreateModal(BaseModel):
    name: constr(min_length=3, max_length=255)
    domains: Optional[List[constr(max_length=255)]] = []
    name_display: NameOptions = NameOptions.first_name_initial
    public_key: constr(min_length=18, max_length=20) = None
    private_key: constr(min_length=20, max_length=50) = None

    @validator('public_key', pre=True, always=True)
    def set_public_key(cls, v):
        return v or token_hex(10)

    @validator('private_key', pre=True, always=True)
    def set_private_key(cls, v):
        return v or token_hex(20)


class CompanyUpdateModel(BaseModel):
    name: constr(min_length=3, max_length=255) = None
    public_key: constr(min_length=18, max_length=20) = None
    private_key: constr(min_length=20, max_length=50) = None

    domains: Optional[List[constr(max_length=255)]] = 'UNCHANGED'

    name_display: NameOptions = None
    show_stars: bool = None
    display_mode: DisplayMode = None
    router_mode: RouterMode = None
    show_hours_reviewed: bool = None
    show_labels: bool = None
    show_location_search: bool = None
    show_subject_filter: bool = None
    terms_link: str = None
    sort_on: SortOn = None
    pagination: int = None
    auth_url: str = None

    class DistanceEnum(str, Enum):
        km = 'km'
        miles = 'miles'

    distance_units: DistanceEnum = None

    class Currency(BaseModel):
        code: str
        symbol: str

    currency: Currency = None


class CompanyOptionsModel(BaseModel):
    """
    Used for options views, this is the definitive set of defaults for company options
    """

    name: str
    name_display: NameOptions

    show_stars: bool = True
    display_mode: DisplayMode = DisplayMode.grid
    router_mode: RouterMode = RouterMode.hash
    show_hours_reviewed: bool = True
    show_labels: bool = True
    show_location_search: bool = True
    show_subject_filter: bool = True
    terms_link: str = None
    sort_on: SortOn = SortOn.name
    pagination: int = 100
    auth_url: str = None
    distance_units: CompanyUpdateModel.DistanceEnum = CompanyUpdateModel.DistanceEnum.miles
    currency: CompanyUpdateModel.Currency = CompanyUpdateModel.Currency(code='GBP', symbol='£')


class ExtraAttributeModel(BaseModel):
    machine_name: str
    name: str
    value: Any
    sort_index: float
    # TutorCruncher sends it only for a Short Textbox set to Show as Link: the text Socket shows instead of the web
    # address ('' to show the address)
    link_text: str = None

    class EATypeEnum(str, Enum):
        checkbox = 'checkbox'
        text_short = 'text_short'
        text_extended = 'text_extended'
        integer = 'integer'
        stars = 'stars'
        dropdown = 'dropdown'
        datetime = 'datetime'
        date = 'date'

    type: EATypeEnum


class ContractorModel(BaseModel):
    id: int
    deleted: bool = False
    first_name: constr(max_length=255) = None
    last_name: constr(max_length=255) = None
    town: constr(max_length=63) = None
    country: constr(max_length=63) = None
    last_updated: datetime = None
    photo: NoneStr = None
    review_rating: float = None
    review_duration: int = None

    @validator('last_updated', pre=True, always=True)
    def set_last_updated(cls, v):
        return v or datetime(2016, 1, 1)

    class LatitudeModel(BaseModel):
        latitude: Optional[float] = None
        longitude: Optional[float] = None

    location: LatitudeModel = None
    extra_attributes: List[ExtraAttributeModel] = []

    class SkillModel(BaseModel):
        subject: str
        subject_id: str
        category: str
        qual_level: str
        qual_level_id: int
        qual_level_ranking: float = 0

    skills: List[SkillModel] = []

    class LabelModel(BaseModel):
        name: str
        machine_name: str

    labels: List[LabelModel] = []


class EnquiryModal(BaseModel):
    client_name: constr(max_length=255)
    client_email: EmailStr = None
    client_phone: Optional[constr(max_length=255)] = None
    service_recipient_name: Optional[constr(max_length=255)] = None
    attributes: Optional[dict] = None
    contractor: Optional[int] = None
    # TODO:
    # subject: Optional[int] = None
    # qual_level: Optional[int] = None
    upstream_http_referrer: Optional[str] = None
    grecaptcha_response: constr(min_length=20)
    terms_and_conditions: bool = False

    @validator('upstream_http_referrer')
    def val_upstream_http_referrer(cls, v):
        return v[:1023]


class AppointmentModel(BaseModel):
    id: int
    service_id: int
    service_name: str
    extra_attributes: List[ExtraAttributeModel]
    colour: str
    appointment_topic: str
    attendees_max: Optional[int]
    attendees_count: int
    attendees_current_ids: List[int]
    start: datetime
    finish: datetime
    price: float
    location: Optional[str]


class BookingModel(BaseModel):
    appointment: int
    student_id: int = None
    student_name: str = ''

    @validator('student_name', always=True)
    def check_name_or_id(cls, v, values, **kwargs):
        if v == '' and values['student_id'] is None:
            raise ValueError('either student_id or student_name is required')
        return v


VIEW_MODELS = {
    'company-create': CompanyCreateModal,
    'company-update': CompanyUpdateModel,
    'webhook-contractor': ContractorModel,
    'enquiry': EnquiryModal,
    'webhook-appointment': AppointmentModel,
    'book-appointment': BookingModel,
}
