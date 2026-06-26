from sqlalchemy import Column, String, Integer, Date
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class AdvertisementActivities(Base):
    __tablename__ = "mcd_advertisement_activities"

    activity_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    action_type_code = Column(Integer)
    action_type_name = Column(String)
    advertisement_id = Column(Integer)
    advertisement_name = Column(String)
    device_type_code = Column(String)
    device_type_name = Column(String)
    market = Column(String)
    reporting_id = Column(UUID(as_uuid=True))
    date = Column(Date)
    activity_source_time_local = Column(TIMESTAMP(timezone=True))
    activity_source_time_utc = Column(TIMESTAMP(timezone=False))