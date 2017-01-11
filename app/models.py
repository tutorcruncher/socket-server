from enum import Enum, unique

from sqlalchemy import Column, Float, ForeignKey, Integer, PrimaryKeyConstraint, Sequence, String
from sqlalchemy import Enum as _SAEnum
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
    key = Column(String(15), nullable=False, unique=True)

    name = Column(String(63))
    site_domain = Column(String(127))

    name_display = Column(sa_enum(NameOptions))


sa_companies = Company.__table__


class Contractor(Base):
    __tablename__ = 'contractors'
    # id set from profile id on TutorCruncher
    id = Column(Integer, primary_key=True, nullable=False)
    company = Column(Integer, ForeignKey('companies.id'), nullable=False)

    first_name = Column(String(63))
    last_name = Column(String(63), nullable=False)

    location = Column(String(63))
    country = Column(String(63))
    latitude = Column(Float())
    longitude = Column(Float())

    tag_line = Column(String(63))

    extra_attributes = Column(JSONB)
    image = Column(String(63))


sa_contractors = Contractor.__table__


class Subject(Base):
    __tablename__ = 'subjects'

    id = Column(Integer, Sequence('subject_id_seq'), primary_key=True, nullable=False)
    name = Column(String(63), nullable=False)
    category = Column(String(63), nullable=False)


sa_subjects = Subject.__table__


class QualLevel(Base):
    __tablename__ = 'qual_levels'

    id = Column(Integer, Sequence('qual_level_id_seq'), primary_key=True, nullable=False)
    name = Column(String(63), nullable=False)
    ranking = Column(Float())


sa_qual_levels = QualLevel.__table__


class ConSkill(Base):
    __tablename__ = 'contractor_skills'

    contractor = Column(Integer, ForeignKey('contractors.id'), nullable=False)
    subject = Column(Integer, ForeignKey('subjects.id'), nullable=False)
    qual_level = Column(Integer, ForeignKey('qual_levels.id'), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('contractor', 'subject', 'qual_level', name='_con_skills'),
    )


sa_con_skills = ConSkill.__table__
