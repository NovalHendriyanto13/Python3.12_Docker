from sqlalchemy import Column, String, Date, Boolean, Integer
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class StampProgramTransactions(Base):
    __tablename__ = "mcd_stamp_program_transactions"

    stamp_program_transaction_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pos_sales_transaction_id = Column(UUID(as_uuid=True))
    sale_id = Column(UUID(as_uuid=True))
    incentive_program_id = Column(Integer)
    incentive_program_name = Column(String, nullable=True)
    transaction_origin = Column(String, nullable=True)
    is_initial_rewarded_stamps = Column(Boolean)
    note = Column(String, nullable=True)
    stamp_card_id = Column(String, nullable=True)
    total_card_stamps_after_transaction = Column(Integer)
    number_of_stamps_added = Column(Integer)
    external_transaction_reference = Column(String, nullable=True)
    venue_id = Column(Integer)
    venue_name = Column(String, nullable=True)
    venue_external_id = Column(String, nullable=True)
    market = Column(String, nullable=True)
    reporting_id = Column(UUID(as_uuid=True))
    date = Column(TIMESTAMP(timezone=True))
    transaction_source_time_local = Column(TIMESTAMP(timezone=True))
    transaction_source_time_utc = Column(TIMESTAMP(timezone=False))
