from sqlalchemy import Column, String, Date, Boolean, Integer
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class LoyaltyActivities(Base):
    __tablename__ = "mcd_loyalty_activities"
    
    activity_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4) 
    reporting_id = Column(UUID(as_uuid=True))
    venue_id = Column(Integer)   
    activity_reward_id = Column(Integer)
    loyalty_program_id = Column(Integer)
    market = Column(String, nullable=True)
    action_type_code = Column(Integer)
    action_type_name = Column(String, nullable=True)
    activated_reward_name = Column(String, nullable=True)
    activity_data = Column(String, nullable=True)
    device_type_code = Column(String, nullable=True)
    device_type_name = Column(Date, nullable=True)
    loyalty_program_name = Column(String, nullable=True)
    num_points = Column(Integer)
    date = Column(Date)
    activity_source_time_local = Column(TIMESTAMP(timezone=True))
    activity_source_time_utc = Column(TIMESTAMP(timezone=True))