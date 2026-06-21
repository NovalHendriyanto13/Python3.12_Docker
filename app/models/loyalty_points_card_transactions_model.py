from sqlalchemy import Column, String
from configs.database import Base

class LoyaltyPointsCardTransactions(Base):
    __tablename__ = "mcd_loyalty_points_card_transactions"
    
    points_card_transaction_id = Column(String, primary_key=True)
    points_card_transaction_group_id = Column(String, nullable=True)
    pos_sales_transaction_id = Column(String, nullable=True)
    transaction_time_utc = Column(String, nullable=True)
    transaction_source_time_local = Column(String, nullable=True)
    transaction_source_time_utc_offset = Column(String, nullable=True)
    transaction_type = Column(String, nullable=True)
    transaction_sub_type = Column(String, nullable=True)
    transaction_origin   = Column(String, nullable=True)
    note = Column(String, nullable=True)
    points_balance_after_transaction   = Column(String, nullable=True)
    reward_type = Column(String, nullable=True)
    reporting_id = Column(String, nullable=True)
    incentive_program_id = Column(String, nullable=True)
    venue_id = Column(String, nullable=True)
    referenced_transactions = Column(String, nullable=True)
    points_delta = Column(String, nullable=True)
    external_transaction_reference   = Column(String, nullable=True)
    sale_id = Column(String, nullable=True)
    venue_external_id = Column(String, nullable=True)
    bonus_points_total = Column(String, nullable=True)