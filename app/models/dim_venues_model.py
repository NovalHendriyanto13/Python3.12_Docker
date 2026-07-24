from sqlalchemy import Column, String, Boolean, Integer
from configs.database import Base

class DimVenues(Base):
    __tablename__ = "dim_venues"
    
    venue_key = Column(Integer, primary_key=True)
    venue_external_id = Column(String)
    market = Column(String)
    name = Column(String)
    venue_type_code = Column(String)
    is_hidden = Column(Boolean)
    region = Column(String)