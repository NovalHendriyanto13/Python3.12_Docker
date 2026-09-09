from sqlalchemy import Column, String, Date, Numeric, Integer
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class SaleDetails(Base):
    __tablename__ = "mcd_sale_details"

    pos_transaction_id = Column(UUID(as_uuid=True), primary_key=True)
    sale_id = Column(UUID(as_uuid=True), primary_key=True)
    reporting_id = Column(UUID(as_uuid=True), primary_key=True)
    offer_id = Column(Integer)
    external_venue_id = Column(UUID(as_uuid=True))
    market = Column(String)
    product_code = Column(String, primary_key=True)
    quantity = Column(Integer)
    unit_price = Column(Numeric(10, 0))
    net_unit_price = Column(Numeric(10, 0))
    tax_unit_amount = Column(Numeric(10, 0))
    line_item_number = Column(String)
    date = Column(Date)
    transaction_source_time_local = Column(TIMESTAMP(timezone=True))
