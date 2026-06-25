from sqlalchemy import Column, String, Boolean, Integer
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP, ARRAY, JSONB
import uuid

class LoyaltyProgramRewards(Base):
    __tablename__ = "mcd_loyalty_program_rewards"

    id = Column(Integer, primary_key=True)
    loyalty_program_id = Column(Integer)
    reward_type = Column(Integer)
    offer_id = Column(Integer)
    expires_after_n_days  = Column(Integer)
    is_expiry_time_specified = Column(Boolean)
    expiry_time_after_activation = = Column(Integer)
    point_value = Column(Integer)
    activation_limit = Column(Integer)
    activation_weight = Column(Integer)
    activation_remained = Column(Integer)
    market = Column(String)