from datetime import datetime
from enum import Enum, unique
from secrets import token_hex
from typing import Any, List, Optional

from pydantic import BaseModel, EmailStr, NoneStr, constr, validator

EXTRA_ATTR_TYPES = 'checkbox', 'text_short', 'text_extended', 'integer', 'stars', 'dropdown', 'datetime', 'date'

MISSING = object()


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


class CompanyCreateModal(BaseModel):
    name: constr(min_length=3, max_length=63)
    name_display: NameOptions = NameOptions.first_name_initial
    url: NoneStr = None
    public_key: constr(min_length=18, max_length=20) = None
    private_key: constr(min_length=20, max_length=50) = None

    @validator('public_key', pre=True, always=True)
    def set_public_key(cls, v):
        return v or token_hex(10)

    @validator('private_key', pre=True, always=True)
    def set_private_key(cls, v):
        return v or token_hex(20)


class CompanyUpdateModel(BaseModel):
    name: constr(min_length=3, max_length=63) = None
    public_key: constr(min_length=18, max_length=20) = None
    private_key: constr(min_length=20, max_length=50) = None

    domains: Optional[List[constr(max_length=255)]] = 'UNCHANGED'
    name_display: NameOptions = None

    show_stars: bool = None
    display_mode: DisplayMode = None
    router_mode: RouterMode = None
    show_hours_reviewed: bool = None
    show_labels: bool = None


class ContractorModel(BaseModel):
    id: int
    deleted: bool = False
    first_name: constr(max_length=63) = None
    last_name: constr(max_length=63) = None
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

    class ExtraAttributeModel(BaseModel):
        machine_name: NoneStr
        name: str
        value: Any
        id: int
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

    @validator('upstream_http_referrer')
    def val_upstream_http_referrer(cls, v):
        return v[:1023]


VIEW_MODELS = {
    'company-create': CompanyCreateModal,
    'company-update': CompanyUpdateModel,
    'webhook-contractor': ContractorModel,
    'enquiry': EnquiryModal,
}
