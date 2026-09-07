from sqlalchemy import Column, String, Date, Boolean, Numeric, Integer, Index
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID
import uuid

class FactSalesSummary(Base):
    __tablename__ = "fact_sales_summary"
    
    sales_summary_key = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    date_key = Column(Integer, unique=True, nullable=True)
    total_amount = Column(Numeric(10, 0))
    gross_amount = Column(Numeric(10, 0))
    tax_total_amount = Column(Numeric(10, 0))
    before_discount_tax_total_amount = Column(Numeric(10, 0))
    before_discount_total_amount = Column(Numeric(10, 0))
    transaction_count = Column(Integer)

    __table_args__ = (
        Index("ix_fact_sale_summary_date", "date_key"),
    )