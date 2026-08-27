from sqlalchemy import Column, String, Integer, UniqueConstraint, Index
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID

class FactConsumerVisitSummary(Base):
    __tablename__ = "fact_consumer_visit_summary"
    
    visit_summary_key = Column(Integer, primary_key=True, autoincrement=True)
    date_key = Column(Integer, nullable=False)
    consumer_key = Column(UUID(as_uuid=True), nullable=False)
    venue_key = Column(Integer, nullable=True)
    total_visit = Column(Integer)
    market = Column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint("date_key", "venue_key", "consumer_key", name="uq_fact_consumer_visit_summary_date_venue_consumer"),
        Index("ix_fact_consumer_visit_summary_consumer", "consumer_key"),
        Index("ix_fact_consumer_visit_summary_date", "date_key")
    )
