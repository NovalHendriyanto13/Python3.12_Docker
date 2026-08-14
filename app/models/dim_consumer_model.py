from sqlalchemy import Column, String, Date, Boolean, SmallInteger, Index
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class DimConsumers(Base):
    __tablename__ = "dim_consumers"
    
    reporting_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    consumer_external_id = Column(String, nullable=True)
    market = Column(String, nullable=True)
    full_name = Column(String, nullable=True)
    phone_number = Column(String, nullable=True)
    email_address = Column(String, nullable=True)
    gender = Column(String, nullable=True)
    date_of_birth = Column(Date, nullable=True)
    postcode = Column(String, nullable=True)
    is_deactivated = Column(Boolean)
    registration_type = Column(SmallInteger) 
    consumer_type = Column(SmallInteger)
    registration_source = Column(String)
    creation_date = Column(TIMESTAMP(timezone=True))
    modified_date = Column(TIMESTAMP(timezone=True))

    __table_args__ = (
        Index("ix_dim_consumer_consumer_external_id", "consumer_external_id"),
        Index("ix_dim_consumer_creation_date", "creation_date"),
        Index("ix_dim_consumer_market", "market"),
    )