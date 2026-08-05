from sqlalchemy import Column, String, BigInteger, Integer, UniqueConstraint
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID

class FactAppActivitySummary(Base):
    __tablename__ = "fact_app_activity_summary"
    
    app_activity_key = Column(BigInteger, primary_key=True, autoincrement=True)
    date_key = Column(Integer, nullable=False)
    consumer_key = Column(UUID(as_uuid=True), nullable=False)
    device = Column(String, nullable=True)
    total_open_apps = Column(Integer)
    market = Column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint("date_key", "device", "consumer_key", name="uq_fact_app_activity_summary"),
    )
