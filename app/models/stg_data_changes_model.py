from sqlalchemy import Column, String, Date, Boolean, Integer, Numeric
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP, JSONB
import uuid

class StgDataChanges(Base):
    __tablename__ = "stg_data_changes"

    id = Column(Integer, primary_key=True)
    module_name = Column(String)
    module_id = Column(Integer)
    old_data = Column(JSONB)
    new_data = Column(JSONB)
