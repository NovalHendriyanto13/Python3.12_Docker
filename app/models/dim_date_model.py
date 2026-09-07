from sqlalchemy import Column, String, Date, SmallInteger, Integer, Index
from configs.database import Base
import uuid

class DimDate(Base):
    __tablename__ = "dim_date"

    date_key = Column(Integer, primary_key=True, autoincrement=True)
    full_date = Column(Date, nullable=False, unique=True)
    day = Column(SmallInteger, nullable=False)
    month = Column(SmallInteger, nullable=False)
    year = Column(SmallInteger, nullable=False)
    quarter = Column(SmallInteger, nullable=False)
    day_string = Column(String, nullable=False)