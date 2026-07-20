from sqlalchemy import Column, String, Date, Boolean, Numeric, Integer
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
from configs.database import Base
import uuid

class FactMicrositeSales(Base):
    __tablename__ = "fact_microsite_sales"

    microsite_sales_key = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    date_key = Column(Integer, nullable=True)
    microsite = Column(String, nullable=False)
    total_amount = Column(Numeric(10, 0))
    gross_amount = Column(Numeric(10, 0))
    tax_total_amount = Column(Numeric(10, 0))
    before_discount_tax_total_amount = Column(Numeric(10, 0))
    before_discount_total_amount = Column(Numeric(10, 0))