from sqlalchemy import Column, Float, Integer, Sequence, String
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Contractor(Base):
    __tablename__ = 'contractors'
    # id set from profile id on TutorCruncher
    id = Column(Integer, primary_key=True, nullable=False)
    first_name = Column(String(63))
    last_name = Column(String(63), nullable=False)

    location = Column(String(63))
    country = Column(String(63))
    latitude = Column(Float())
    longitude = Column(Float())

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
    __tablename__ = 'con_skills'

    id = Column(Integer, Sequence('con_skill_id_seq'), primary_key=True, nullable=False)
    contractor = Column(Integer, ForeignKey('contractors.id'), nullable=False)
    subject = Column(Integer, ForeignKey('subjects.id'), nullable=False)
    qual_level = Column(Integer, ForeignKey('qual_levels.id'), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ('contractor', 'subject', 'qual_level'),
            ('contractors.id',),
            name='_con_skills'
        ),
    )


sa_con_skills = ConSkill.__table__
