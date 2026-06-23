from sqlalchemy import Column, String, Boolean, Integer, Numeric
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class SalesHeaders(Base):
    __tablename__ = "mcd_sales_headers"

    sale_id = Column(String, nullable=False)
    reporting_id = Column(UUID(as_uuid=True))
    total_amount = Column(Numeric)
    offer_ids = Column(Integer)
    internal_id = Column(String, nullable=False)
    gross_amount = Column(Numeric)
    tax_total_amount = Column(Numeric)
    before_discount_tax_total_amount = Column(Numeric)
    before_discount_total_amount Column(Numeric)
    day_part = Column(String, nullable=False)
    pod_type = Column(String)
    transaction_kind = Column(String)
    order_take_platform = Column(String)
    sale_type = Column(String)
    venue_id = Column(Integer)
    date_occurred = Column(TIMESTAMP(timezone=True))
    pos_transaction_id = Column(UUID(as_uuid=True))        