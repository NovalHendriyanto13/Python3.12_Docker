from sqlalchemy import Column, String, SmallInteger, Date, Integer, Boolean
from configs.database import Base
from sqlalchemy.dialects.postgresql import TIMESTAMP

class Advertisements(Base):
    __tablename__ = "mcd_advertisements"
    
    advertisement_id = Column(Integer, primary_key=True)
    campaign_id = Column(Integer)
    market = Column(String)
    name = Column(String)
    title = Column(String)
    description = Column(String)
    click_through_url = Column(String)
    status = Column(Integer)
    channel_code = Column(String)
    placement_code = Column(String)
    no_compete_group = Column(String)
    enable_time_based_weight = Column(Boolean)
    enable_distance_weight = Column(Boolean)
    apply_geo_fence_filters = Column(Boolean)
    apply_tag_value_filter = Column(Boolean)
    weight = Column(SmallInteger)
    days_of_week = Column(String)
    daily_start_time = Column(SmallInteger)
    daily_end_time = Column(SmallInteger)
    date_modified = Column(TIMESTAMP(timezone=True))
    date_created = Column(TIMESTAMP(timezone=True))
    start_date = Column(TIMESTAMP(timezone=True))
    end_date = Column(TIMESTAMP(timezone=True))