from sqlalchemy import Column, String, Date, Boolean, Numeric, Integer, UniqueConstraint, Index
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID

class FactConsumerSalesSummary(Base):
    __tablename__ = "fact_consumer_sales_summary"
    
    consumer_sales_summary_key = Column(Integer, primary_key=True, autoincrement=True)
    date_key = Column(Integer, nullable=False)
    consumer_key = Column(UUID(as_uuid=True), nullable=False)
    venue_key = Column(Integer, nullable=False)
    total_amount = Column(Numeric(10, 0))
    gross_amount = Column(Numeric(10, 0))
    tax_total_amount = Column(Numeric(10, 0))
    before_discount_tax_total_amount = Column(Numeric(10, 0))
    before_discount_total_amount = Column(Numeric(10, 0))
    guest_count = Column(Integer)

    __table_args__ = (
        UniqueConstraint("date_key", "venue_key", "consumer_key", name="uq_fact_consumer_sales_summary_date_venue_consumer"),
        Index("ix_fact_consumer_sales_consumer_date_key", "consumer_key", "date_key")
    )