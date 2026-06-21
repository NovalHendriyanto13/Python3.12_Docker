from sqlalchemy import Column, String
from configs.database import Base

class Advertisements(Base):
    __tablename__ = "mcd_advertisements"
    
    id = Column(String, primary_key=True)      
    campaignid = Column(String, nullable=True)
    title = Column(String, nullable=True)
    description = Column(String, nullable=True)
    startdate = Column(String, nullable=True)
    enddate = Column(String, nullable=True)
    status = Column(String, nullable=True)