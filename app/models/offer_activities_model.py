from sqlalchemy import Column, String, Date, Boolean, Integer
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class OfferActivities(Base):
    __tablename__ = "mcd_offer_activities"
    
    activity_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)  
    activity_data = Column(String, nullable=True)
    action_type_code = Column(Integer) 
    action_type_name = Column(String, nullable=True)
    device_type_code = Column(String, nullable=True)
    device_type_name = Column(String, nullable=True)
    offer_id = Column(Integer) 
    offer_name = Column(String, nullable=True)
    venue_id = Column(Integer) 
    market = Column(String, nullable=True)
    reporting_id = Column(UUID(as_uuid=True)) 
    date = Column(Date)
    activity_source_time_local = Column(TIMESTAMP(timezone=True))
    activity_source_time_utc = Column(TIMESTAMP(timezone=False))
