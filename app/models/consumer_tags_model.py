from sqlalchemy import Column, String
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class ConsumerTags(Base):
    __tablename__ = "mcd_consumer_tags"
    
    reporting_id = Column(UUID(as_uuid=True), primary_key=True)
    tag_value_id = Column(UUID(as_uuid=True), primary_key=True)
    tag_assigned_time_utc = Column(TIMESTAMP(timezone=True))
    market = Column(String)
    reference_code = Column(String)
