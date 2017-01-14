from enum import Enum, unique

from sqlalchemy import Enum as _SAEnum
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


@unique
class NameOptions(str, Enum):
    first_name = 'first_name'
    first_name_initial = 'first_name_initial'
    full_name = 'full_name'


def sa_enum(enum: Enum):
    return _SAEnum(*(v.value for v in enum.__members__.values()), name=enum.__name__.lower())


class Company(Base):
    __tablename__ = 'companies'
    # id set from profile id on TutorCruncher
    id = Column(Integer, primary_key=True, nullable=False)
    key = Column(String(20), index=True, nullable=False, unique=True)

    name = Column(String(63), unique=True)
    site_domain = Column(String(63))

    name_display = Column(sa_enum(NameOptions), default=NameOptions.first_name_initial, nullable=False)


sa_companies = Company.__table__


@unique
class Action(str, Enum):
    insert = 'insert'
    update = 'update'


class Contractor(Base):
    __tablename__ = 'contractors'
    # id set from profile id on TutorCruncher
    id = Column(Integer, primary_key=True, autoincrement=False, nullable=False)
    company = Column(Integer, ForeignKey('companies.id'), nullable=False)

    first_name = Column(String(63), index=True)
    last_name = Column(String(63))

    town = Column(String(63))
    country = Column(String(63))
    latitude = Column(Float())
    longitude = Column(Float())

    tag_line = Column(String(63))
    primary_description = Column(Text())

    extra_attributes = Column(JSONB)
    photo = Column(String(63))

    last_updated = Column(DateTime, nullable=False, index=True)
    action = Column(sa_enum(Action), default=Action.insert, nullable=False)


sa_contractors = Contractor.__table__


class Subject(Base):
    __tablename__ = 'subjects'

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(63), nullable=False, index=True)
    category = Column(String(63), nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint('name', 'category', name='_subject_name_cat'),
    )


sa_subjects = Subject.__table__


class QualLevel(Base):
    __tablename__ = 'qual_levels'

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(63), nullable=False, unique=True, index=True)
    ranking = Column(Float())


sa_qual_levels = QualLevel.__table__


class ConSkill(Base):
    __tablename__ = 'contractor_skills'

    id = Column(Integer, primary_key=True, nullable=False)
    contractor = Column(Integer, ForeignKey('contractors.id'), nullable=False)
    subject = Column(Integer, ForeignKey('subjects.id'), nullable=False)
    qual_level = Column(Integer, ForeignKey('qual_levels.id'), nullable=False)

    __table_args__ = (
        UniqueConstraint('contractor', 'subject', 'qual_level', name='_con_skill_all'),
    )


sa_con_skills = ConSkill.__table__
