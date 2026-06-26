from sqlalchemy import Column, String, Date, Boolean, Integer
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class PushmessageActivities(Base):
    __tablename__ = "mcd_pushmessage_activities"

    activity_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    activity_data = Column(String, nullable=True)
    action_type_code = Column(Integer)
    action_type_name = Column(String, nullable=True)
    device_type_code = Column(String, nullable=True)
    device_type_name = Column(String, nullable=True)
    message_id = Column(Integer)
    message_name = Column(String, nullable=True)
    num_messages = Column(Integer)
    market = Column(String, nullable=True)
    reporting_id = Column(UUID(as_uuid=True))
    date = Column(Date)
    activity_source_time_local = Column(TIMESTAMP(timezone=True))
    activity_source_time_utc = Column(TIMESTAMP(timezone=False))
