from sqlalchemy import Column, String, Date, Boolean, Numeric, Integer, UniqueConstraint
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID

class FactConsumerSalesHeaderSummary(Base):
    __tablename__ = "fact_consumer_sales_header_summary"
    
    consumer_sales_header_summary_key = Column(Integer, primary_key=True, autoincrement=True)
    date_key = Column(Integer, nullable=False)
    consumer_key = Column(UUID(as_uuid=True), nullable=False)
    venue_key = Column(Integer, nullable=False)
    day_part = Column(String, nullable=True)
    pod_type = Column(String, nullable=True)
    transaction_kind = Column(String, nullable=True)
    order_take_platform = Column(String, nullable=True)
    sale_type = Column(String, nullable=True)
    total_amount = Column(Numeric(10, 0))
    gross_amount = Column(Numeric(10, 0))
    tax_total_amount = Column(Numeric(10, 0))
    before_discount_tax_total_amount = Column(Numeric(10, 0))
    before_discount_total_amount = Column(Numeric(10, 0))

    __table_args__ = (
        UniqueConstraint(
            "date_key", "venue_key", "consumer_key", "transaction_kind",
            "day_part", "pod_type", "order_take_platform", "sale_type",
            name="uq_fact_sales_header_summary"
        ),
    )