from sqlalchemy import Column, String
from configs.database import Base

class Offers(Base):
    __tablename__ = "mcd_offers"
    
    id = Column(String, primary_key=True)             
    campaignid = Column(String, nullable=True)
    title = Column(String, nullable=True)    
    description = Column(String, nullable=True)
    category = Column(String, nullable=True)
    codetype = Column(String, nullable=True)
    status = Column(String, nullable=True)
    isreward = Column(String, nullable=True)
    whenstarts = Column(String, nullable=True)
    whenexpires = Column(String, nullable=True)
    whenlastupdated = Column(String, nullable=True)
    redemptiontext = Column(String, nullable=True)