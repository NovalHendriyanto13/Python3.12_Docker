from sqlalchemy import Column, String, Date, Numeric, Integer
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class StampProgramRewardTransactions(Base):
    __tablename__ = "mcd_stamp_program_reward_transactions"

    stamp_program_reward_transaction_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4) 
    stamp_program_reward_transaction_group_id = Column(UUID(as_uuid=True))
    pos_sales_transaction_id = Column(UUID(as_uuid=True))
    sale_id = Column(UUID(as_uuid=True))
    incentive_program_id = Column(Integer)
    incentive_program_name = Column(String)
    stamp_reward_transaction_type = Column(String)
    stamp_card_id = Column(String)
    card_capacity = Column(Integer)
    reward_type = Column(String)
    reward_reference_id = Column(String)
    venue_id = Column(Integer)
    venue_name = Column(String)
    venue_external_id = Column(String)
    market = Column(String)
    reporting_id = Column(UUID(as_uuid=True))
    date = Column(Date)
    transaction_source_time_local = Column(TIMESTAMP(timezone=True))
    transaction_time_utc = Column(TIMESTAMP)
