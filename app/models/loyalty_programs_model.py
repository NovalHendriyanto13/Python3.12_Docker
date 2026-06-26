from sqlalchemy import Column, String, Date, Boolean, Integer
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class LoyaltyPrograms(Base):
    __tablename__ = "mcd_loyalty_programs"
    
    loyalty_program_id = Column(Integer, primary_key=True)
    campaign_id = Column(Integer)
    category_id = Column(Integer)
    max_instances = Column(Integer)
    extended_data = Column(String)
    name = Column(String)
    title = Column(String)
    sub_title = Column(String)
    description = Column(String)
    instructions = Column(String)
    status = Column(Integer)
    terms_and_conditions = Column(String)
    points_required = Column(Integer)
    days_of_week = Column(String)
    weighting = Column(Integer)
    daily_start_time = Column(Integer)
    daily_end_time = Column(Integer)
    max_points_per_day = Column(Integer)
    apply_initial_points_to_subsequent_cards = Column(Boolean)
    max_points_requests_per_day = Column(Integer)
    initial_points = Column(Integer)
    is_hidden = Column(Boolean)
    require_ip_whitelisting = Column(Boolean)
    loyalty_program_type = Column(Integer)
    points_expiry_days = Column(Integer)
    expiry_schedule_details = Column(String)
    is_consumer_api_write_accessible = Column(Boolean)
    market = Column(String)
    start_date = Column(TIMESTAMP(timezone=True))
    end_date = Column(TIMESTAMP(timezone=True))
    when_last_updated = Column(TIMESTAMP(timezone=True))