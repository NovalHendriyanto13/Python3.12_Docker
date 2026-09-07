from sqlalchemy import Column, String, Integer, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import UUID
from configs.database import Base
import uuid

class FactAppActivitySummary(Base):
    __tablename__ = "fact_app_activity_summary"

    app_activity_key = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    date_key = Column(Integer, nullable=False)
    activity = Column(String, nullable=False)
    device = Column(String, nullable=True)
    total_count = Column(Integer)
    consumer_count = Column(Integer)
    market = Column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint("date_key", "activity", name="uq_fact_app_activity_summary"),
        Index("ix_fact_app_activity_summary_date", "date_key"),
    )
