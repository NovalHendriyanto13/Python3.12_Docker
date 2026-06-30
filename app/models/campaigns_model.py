from sqlalchemy import Column, String, SmallInteger, Date, Integer, Numeric
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP, ARRAY, JSONB
import uuid

class Campaigns(Base):
    __tablename__ = "mcd_campaigns"
    
    campaign_id = Column(Integer, primary_key=True)
    market = Column(String)
    title = Column(String)
    campaign_status = Column(Integer)
    status = Column(String)
    creation_date = Column(TIMESTAMP(timezone=True))
    modified_date = Column(TIMESTAMP(timezone=True))