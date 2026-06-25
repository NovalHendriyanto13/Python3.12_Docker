from sqlalchemy import Column, String, Date, Boolean, Integer
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class Consumers(Base):
    __tablename__ = "mcd_consumers"

    message_id = Column(Integer, primary_key=True)    
    market = Column(String, nullable=True)
    status = Column(Integer) 
    name = Column(String, nullable=True)
    subject = Column(String, nullable=True)
    body = Column(String, nullable=True)
    email_text_body = Column(String, nullable=True)
    offer_id = Column(Integer) 
    recurring_type_code = Column(Integer) 
    daily_start_time = Column(Integer) 
    daily_end_time = Column(Integer) 
    time_frame_type = Column(Integer) 
    date_modified = Column(TIMESTAMP(timezone=True))
    date_created = Column(TIMESTAMP(timezone=True))
    time_frame_start_date = Column(TIMESTAMP(timezone=True))
    time_frame_end_date = Column(TIMESTAMP(timezone=True))