from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import relationship, sessionmaker
from contextlib import contextmanager
from . import db_settings

Base = declarative_base()

def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine(URL.create(**db_settings.DATABASE))

# Create a session factory using the engine
SessionFactory = sessionmaker(bind=db_connect())

@contextmanager
def session_scope():
    """
    Provide a transactional scope around a series of operations.
    """
    session = SessionFactory()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def create_table(engine):
    Base.metadata.create_all(engine)

class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True)
    name = Column('name', String)
    price = Column('price', Float)
    tag = Column('tag', String)
    test = Column('test', String)


class Properties(Base):
    __tablename__ = "properties"
    id = Column(Integer, primary_key=True)
    rooms = Column('rooms', Integer)
    garage = Column('garage', String)
    balcony_number = Column('balconyNumber', Integer)
    area = Column('area', Float)
    location = Column('location', String)
    door = Column('door', String)
    window = Column('window', String)
    floor = Column('floor', String)
    window_number = Column('windowNumber', Integer)
    building_floor = Column('buildingFloor', Integer)
    which_floor = Column('whichFloor', Integer)
    commission_year = Column('commissionYear', Integer)
    leasing = Column('leasing', String)
    progress = Column('progress', String)
    price = Column('price', Float)
    province = Column('province', String)
    district = Column('district', String)
    khoroo = Column('khoroo', String)
    property_type = Column('propertyType', String)
    sell_type = Column('sellType', String)


    images = relationship("PropertyImage", back_populates="property")

class PropertyImage(Base):
    __tablename__ = 'property_images'

    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey('properties.id'))
    img_url = Column(Text)

    property = relationship("Properties", back_populates="images")
