from enum import Enum, unique
from datetime import datetime
from typing import Any, Optional, List

from pydantic import BaseModel, EmailStr, NoneStr, constr

EXTRA_ATTR_TYPES = 'checkbox', 'text_short', 'text_extended', 'integer', 'stars', 'dropdown', 'datetime', 'date'

MISSING = object()


@unique
class NameOptions(str, Enum):
    first_name = 'first_name'
    first_name_initial = 'first_name_initial'
    full_name = 'full_name'


class CompanyCreateModal(BaseModel):
    name: constr(min_length=3, max_length=63)
    name_display: NameOptions = NameOptions.first_name_initial
    url: NoneStr = None
    public_key: Optional[constr(min_length=18, max_length=20)] = None
    private_key: Optional[constr(min_length=20, max_length=50)] = None


class CompanyUpdateModel(BaseModel):
    name: constr(min_length=3, max_length=63) = None
    name_display: NameOptions = None
    url: NoneStr = None
    private_key: str = None


class LatitudeModel(BaseModel):
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class EATypeEnum(str, Enum):
    checkbox = 'checkbox'
    text_short = 'text_short'
    text_extended = 'text_extended'
    integer = 'integer'
    stars = 'stars'
    dropdown = 'dropdown'
    datetime = 'datetime'
    date = 'date'


class ExtraAttributeModel(BaseModel):
    machine_name: NoneStr
    type: EATypeEnum
    name: str
    value: Any
    id: int
    sort_index: float


class SkillModel(BaseModel):
    subject: str
    subject_id: str
    category: str
    qual_level: str
    qual_level_id: int
    qual_level_ranking: float = 0


class ContractorModel(BaseModel):
    id: int
    deleted: bool = False
    first_name: constr(max_length=63) = None
    last_name: constr(max_length=63) = None
    town: constr(max_length=63) = None
    country: constr(max_length=63) = None
    location: LatitudeModel = None
    extra_attributes: List[ExtraAttributeModel] = []
    skills: List[SkillModel] = []
    last_updated: datetime = None
    photo: NoneStr = None


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
    upstream_http_referrer: Optional[constr(max_length=1023)] = None
    grecaptcha_response: constr(min_length=20, max_length=1000)


VIEW_MODELS = {
    'company-create': CompanyCreateModal,
    'company-update': CompanyUpdateModel,
    'contractor-set': ContractorModel,
    'enquiry': EnquiryModal,
}
