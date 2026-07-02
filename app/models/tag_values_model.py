from sqlalchemy import Column, String, Boolean
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class TagValues(Base):
    __tablename__ = "mcd_tag_values"

    tag_value_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4) 
    name = Column(String)
    reference_code = Column(String)
    consumer_visible = Column(Boolean)
    consumer_updateable = Column(Boolean)
    market = Column(String)

