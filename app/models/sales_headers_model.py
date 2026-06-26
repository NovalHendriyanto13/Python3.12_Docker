from sqlalchemy import Column, String, Boolean, Integer, Numeric
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class SalesHeaders(Base):
    __tablename__ = "mcd_sales_headers"

    sale_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pos_transaction_id = Column(UUID(as_uuid=True))
    reporting_id = Column(UUID(as_uuid=True))
    offer_ids = Column(String)
    venue_external_id = Column(String)
    total_amount = Column(Numeric(10, 0))
    tax_total_amount = Column(Numeric(10, 0))
    gross_amount = Column(Numeric(10, 0))
    before_discount_tax_total_amount = Column(Numeric(10, 0))
    before_discount_total_amount = Column(Numeric(10, 0))
    day_part = Column(String)
    pod_type = Column(String)
    transaction_kind = Column(String)
    order_take_platform = Column(String)
    sale_type = Column(String)
    date = Column(Date)
    market = Column(String)
    transaction_source_time_local = Column(TIMESTAMP(timezone=True))
    plexure_processing_time_utc = Column(TIMESTAMP)  