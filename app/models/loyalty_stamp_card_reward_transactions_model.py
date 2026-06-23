from sqlalchemy import Column, String, Boolean, SmallInteger, Integer
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class LoyaltyStampCardRewardTransactions(Base):
    __tablename__ = "mcd_loyalty_stamp_card_reward_transactions"

    stamp_card_reward_transaction_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    stamp_card_reward_transaction_group_id = Column(UUID(as_uuid=True))
    pos_sales_transaction_id = Column(UUID(as_uuid=True))
    transaction_time_utc = Column(TIMESTAMP(timezone=True))      
    transaction_source_time_local = Column(TIMESTAMP(timezone=True))
    transaction_source_time_utc_offset = Column(String, nullable=False)
    stamp_reward_transaction_type = Column(String, nullable=False)
    note = Column(String, nullable=True)                        
    stamp_card_id = Column(UUID(as_uuid=True))                    
    reporting_id = Column(UUID(as_uuid=True))                  
    incentive_program_id = Column(Integer)       
    venue_id = Column(String, nullable=True)                      
    sale_id = Column(String, nullable=True)
    venue_external_id = Column(String, nullable=True)           