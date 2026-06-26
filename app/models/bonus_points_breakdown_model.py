from sqlalchemy import Column, String, SmallInteger, Date, Integer, Numeric
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP, ARRAY, JSONB
import uuid

class BonusPointsBreakdown(Base):
    __tablename__ = "mcd_bonus_points_breakdown"
    
    points_program_transaction_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pos_sales_transaction_id = Column(String)
    ordering_method = Column(ARRAY(String))
    day_of_week = Column(ARRAY(String))
    time_of_day = Column(ARRAY(String))
    minimum_spend = Column(ARRAY(Numeric(10, 0)))
    product = Column(ARRAY(JSONB))
    satisfied_condition_type = Column(ARRAY(String))
    bonus_rule_id = Column(String)
    bonus_points = Column(Integer)
    standard_points = Column(Integer)
    venue_id = Column(Integer)
    venue_name = Column(String)
    venue_external_id = Column(String)
    market = Column(String)
    reporting_id = Column(UUID(as_uuid=True))
    date = Column(Date)
    transaction_source_time_local = Column(TIMESTAMP(timezone=True))
    transaction_time_utc = Column(TIMESTAMP(timezone=False))
    bonus_points_breakdown_position = Column(Integer)