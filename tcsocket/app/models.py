from enum import Enum, unique
from typing import Type

from sqlalchemy import Column, DateTime, Enum as _SAEnum, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.declarative import declarative_base

from .validation import NameOptions

Base = declarative_base()


def sa_enum(enum: Type[Enum]):
    return _SAEnum(*(v.value for v in enum.__members__.values()), name=enum.__name__.lower())


class Company(Base):
    __tablename__ = 'companies'
    id = Column(Integer, primary_key=True, nullable=False)
    public_key = Column(String(20), index=True, nullable=False, unique=True)
    private_key = Column(String(50), index=True, nullable=False)

    name = Column(String(63), unique=True)
    domains = Column(ARRAY(String(255)))

    name_display = Column(sa_enum(NameOptions), default=NameOptions.first_name_initial, nullable=False)

    options = Column(JSONB)


sa_companies = Company.__table__


@unique
class Action(str, Enum):
    created = 'created'
    updated = 'updated'
    deleted = 'deleted'


class Contractor(Base):
    __tablename__ = 'contractors'
    # id set from profile id on TutorCruncher
    id = Column(Integer, primary_key=True, autoincrement=False, nullable=False)
    company = Column(Integer, ForeignKey('companies.id'), nullable=False)

    first_name = Column(String(255), index=True)
    last_name = Column(String(255))

    town = Column(String(63))
    country = Column(String(63))
    latitude = Column(Float)
    longitude = Column(Float)

    tag_line = Column(String(255))
    primary_description = Column(Text())

    extra_attributes = Column(JSONB)

    last_updated = Column(DateTime, nullable=False, index=True)
    action = Column(sa_enum(Action), default=Action.created, nullable=False)
    labels = Column(ARRAY(String(255)))

    review_rating = Column(Float)
    review_duration = Column(Integer, nullable=False, server_default='0')
    photo_hash = Column(String(6), nullable=False, server_default='-')


sa_contractors = Contractor.__table__


class Subject(Base):
    __tablename__ = 'subjects'

    id = Column(Integer, primary_key=True, autoincrement=False, nullable=False)
    name = Column(String(63), nullable=False)
    category = Column(String(63), nullable=False)


sa_subjects = Subject.__table__


class QualLevel(Base):
    __tablename__ = 'qual_levels'

    id = Column(Integer, primary_key=True, autoincrement=False, nullable=False)
    name = Column(String(63), nullable=False)
    ranking = Column(Float)


sa_qual_levels = QualLevel.__table__


class ConSkill(Base):
    __tablename__ = 'contractor_skills'

    id = Column(Integer, primary_key=True, nullable=False)
    contractor = Column(Integer, ForeignKey('contractors.id', ondelete='CASCADE'), nullable=False)
    subject = Column(Integer, ForeignKey('subjects.id'), nullable=False)
    qual_level = Column(Integer, ForeignKey('qual_levels.id'), nullable=False)

    __table_args__ = (UniqueConstraint('contractor', 'subject', 'qual_level', name='_con_skill_all'),)


sa_con_skills = ConSkill.__table__


class Label(Base):
    __tablename__ = 'labels'

    id = Column(Integer, primary_key=True, nullable=False)
    company = Column(Integer, ForeignKey('companies.id'), nullable=False)
    machine_name = Column(String(255), nullable=False)
    name = Column(String(255), nullable=False)

    __table_args__ = (UniqueConstraint('company', 'machine_name', name='_labels_company_machine_name'),)


sa_labels = Label.__table__


class Service(Base):
    __tablename__ = 'services'

    id = Column(Integer, primary_key=True, nullable=False)
    company = Column(Integer, ForeignKey('companies.id'), nullable=False)

    name = Column(String(255), nullable=False)
    colour = Column(String(20), nullable=False)
    extra_attributes = Column(JSONB)


sa_services = Service.__table__


class Appointment(Base):
    __tablename__ = 'appointments'

    id = Column(Integer, primary_key=True, nullable=False)
    service = Column(Integer, ForeignKey('services.id', ondelete='RESTRICT'), nullable=False)

    topic = Column(String(255), nullable=False)
    attendees_max = Column(Integer)
    attendees_count = Column(Integer, nullable=False)
    attendees_current_ids = Column(ARRAY(Integer), nullable=False)

    # index so it can be used to delete old appointments
    start = Column(DateTime, nullable=False, index=True)
    finish = Column(DateTime, nullable=False)

    price = Column(Float)
    location = Column(String(255))


sa_appointments = Appointment.__table__
