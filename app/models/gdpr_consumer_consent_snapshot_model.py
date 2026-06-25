from sqlalchemy import Column, String, Date, Boolean, SmallInteger
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP, JSONB
import uuid

class GdprConsumerConsentSnapshot(Base):
    __tablename__ = "mcd_gdpr_consumer_consent_snapshot"
    
    reporting_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)    
    market = Column(String, nullable=True)
    consent_to_store_and_process = Column(Boolean)
    services = Column(JSONB)
    event_time_utc = Column(TIMESTAMP)
    date = Column(TIMESTAMP(timezone=True))
    
    
    