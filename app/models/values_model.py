from sqlalchemy import Column, String, Date, Boolean, Integer, Numeric
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class Venues(Base):
    __tablename__ = "mcd_venues"

    venue_id = Column(Integer, primary_key=True)
    venue_external_id
    region_id = Column(Integer)
    market = Column(String)
    name = Column(String)
    location_lat = Column(Numeric(10, 2))
    location_long = Column(Numeric(10, 2))
    address_line1 = Column(String)
    address_line2 = Column(String)
    address_line3 = Column(String)
    post_code = Column(String)
    venue_type_code = Column(String)
    is_hidden = Column(Boolean)
    region = Column(String)
    features = Column(String)
    extended_data = Column(String)
    lab = Column(Boolean)
    accepts_offers = Column(Boolean)
    time_zone = Column(String)
    open_hours = Column(String)
    when_last_updated = Column(TIMESTAMP(timezone=True))