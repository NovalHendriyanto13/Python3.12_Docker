from sqlalchemy import Column, String
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class DimProductPillars(Base):
    __tablename__ = "dim_product_pillars"
    
    product_pillar_key = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pillar_name = Column(String, nullable=False)
    pillar_description = Column(String, nullable=False)