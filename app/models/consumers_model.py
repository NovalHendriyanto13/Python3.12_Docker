from sqlalchemy import Column, String
from configs.database import Base

class Consumers(Base):
    __tablename__ = "mcd_consumer"
    
    reportingid = Column(String, primary_key=True)    
    firstname = Column(String, nullable=True)
    lastname = Column(String, nullable=True)
    fullname = Column(String, nullable=True)
    emailaddress = Column(String, nullable=True)
    registrationtype = Column(String, nullable=True) 
    postcode = Column(String, nullable=True)
    creationdate = Column(String, nullable=True)
    modifieddate = Column(String, nullable=True)
    dateofbirth = Column(String, nullable=True)
    gender = Column(String, nullable=True)
    isdeactivated = Column(String, nullable=True)
    deactivationdate = Column(String, nullable=True)
    lastknowndeviceid = Column(String, nullable=True)
    phonenumber = Column(String, nullable=True)
    consumertype = Column(String, nullable=True)