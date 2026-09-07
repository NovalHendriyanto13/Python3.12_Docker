from sqlalchemy import Column, String, Date, Boolean, Numeric, Integer, UniqueConstraint, Enum
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID
from enum import Enum as PyEnum
import uuid

class EnumTransactionType(PyEnum):
    INCENTIVISED = "incentivised"
    UNINCENTIVISED = "unincentivised"
    REWARD = "reward"

class FactTrxTypeSalesSummary(Base):
    __tablename__ = "fact_transaction_type_sales_summary"
    
    transaction_type_sales_summary_key = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
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
    transaction_type = Column(Enum(EnumTransactionType), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "date_key", "venue_key", "consumer_key", "transaction_kind",
            "day_part", "pod_type", "order_take_platform", "sale_type",
            name="uq_fact_sales_header_summary"
        ),
    )