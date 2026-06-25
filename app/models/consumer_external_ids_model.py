from sqlalchemy import Column, String, SmallInteger, Date, Boolean
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class ConsumerExternalIds(Base):
    __tablename__ = "mcd_consumer_external_ids"
    
    reporting_id = Column(UUID(as_uuid=True))
    external_id = Column(String)
    market = Column(String)
    deleted_flag = Column(Boolean)
    timestamp = Column(TIMESTAMP(timezone=True))
