from sqlalchemy import Column, String, Boolean, SmallInteger, Integer
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class LoyaltyStampCardTransactions(Base):
    __tablename__ = "mcd_loyalty_stamp_card_transactions"

    stamp_card_transaction_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pos_sales_transaction_id = Column(UUID(as_uuid=True))
    transaction_time_utc = Column(TIMESTAMP(timezone=True))      
    transaction_source_time_local = Column(TIMESTAMP(timezone=True))
    transaction_source_time_utc_offset = Column(String, nullable=False)
    transaction_origin = Column(String, nullable=False)
    is_initial_rewarded_stamps = Column(Boolean)
    note = Column(String, nullable=True)                        
    stamp_card_id = Column(UUID(as_uuid=True))                    
    total_card_stamps_after_transaction = Column(SmallInteger)
    reporting_id = Column(UUID(as_uuid=True))                  
    incentive_program_id = Column(Integer)       
    venue_id = Column(String, nullable=True)                      
    number_of_stamps_added = Column(SmallInteger)      
    external_transaction_reference = Column(String, nullable=True) 
    sale_id = Column(String, nullable=True)
    venue_external_id = Column(String, nullable=True)           