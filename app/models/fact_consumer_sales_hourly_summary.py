from sqlalchemy import Column, Numeric, Integer, UniqueConstraint, Index
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID
import uuid

class FactConsumerSalesHourlySummary(Base):
    __tablename__ = "fact_consumer_sales_hourly_summary"

    consumer_sales_hourly_summary_key = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    date_key = Column(Integer, nullable=False)
    hour = Column(Integer, nullable=False)
    consumer_key = Column(UUID(as_uuid=True), nullable=False)
    venue_key = Column(Integer, nullable=False)
    total_amount = Column(Numeric(10, 0))
    gross_amount = Column(Numeric(10, 0))
    tax_total_amount = Column(Numeric(10, 0))
    before_discount_tax_total_amount = Column(Numeric(10, 0))
    before_discount_total_amount = Column(Numeric(10, 0))
    transaction_count = Column(Integer)

    __table_args__ = (
        UniqueConstraint(
            "date_key", "hour", "venue_key", "consumer_key",
            name="uq_fact_consumer_sales_hourly_date_hour_venue_consumer"
        ),
        Index("ix_fact_consumer_sales_hourly_consumer_date_hour", "consumer_key", "date_key", "hour")
    )
