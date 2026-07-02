from sqlalchemy import Column, String, Date, Boolean, Integer
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP, ARRAY, JSONB
import uuid

class PointsProgramTransactions(Base):
    __tablename__ = "mcd_points_program_transactions"
    
    points_program_transaction_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    points_program_transaction_group_id = Column(UUID(as_uuid=True))
    pos_sales_transaction_id = Column(UUID(as_uuid=True))
    sale_id = Column(UUID(as_uuid=True))
    incentive_program_id = Column(Integer)
    incentive_program_name = Column(String, nullable=True)
    transaction_type = Column(String, nullable=True)
    transaction_sub_type = Column(String, nullable=True)
    transaction_origin = Column(String, nullable=True)
    note = Column(String, nullable=True)
    points_balance_after_transaction = Column(Integer)
    points_delta = Column(Integer)
    pointsrequested = Column(Integer)
    external_transaction_reference = Column(String, nullable=True)
    referenced_transactions = Column(ARRAY(JSONB))
    reward_type = Column(String, nullable=True)
    reward_reference_id = Column(UUID(as_uuid=True))
    venue_id = Column(Integer)
    venue_name = Column(String, nullable=True)
    venue_external_id = Column(String, nullable=True)
    market = Column(String, nullable=True)
    reporting_id = Column(UUID(as_uuid=True))
    date = Column(Date)
    transaction_source_time_local = Column(TIMESTAMP(timezone=True))
    transaction_time_utc = Column(TIMESTAMP(timezone=False))