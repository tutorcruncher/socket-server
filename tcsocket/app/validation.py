import logging
from datetime import datetime
from enum import Enum, unique
from secrets import token_hex
from typing import Any, List, Optional

from pydantic import BaseModel, EmailStr, NoneStr, constr, root_validator, validator

EXTRA_ATTR_TYPES = 'checkbox', 'text_short', 'text_extended', 'integer', 'stars', 'dropdown', 'datetime', 'date'

MISSING = object()

logger = logging.getLogger('socket')


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

    @root_validator(pre=True)
    def set_last_updated(cls, values):
        """get the release_timestamp and save it to the last_updated field"""

        if 'release_timestamp' not in values:
            logger.warning('release_timestamp not found in values, setting last_updated to 2016-01-01')

        values['last_updated'] = values.get('release_timestamp', datetime(2016, 1, 1))
        return values

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
    grecaptcha_response: constr(min_length=20, max_length=1000)
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
