from sqlalchemy import Column, String, UniqueConstraint
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID
import uuid

class DimPillars(Base):
    __tablename__ = "dim_pillars"

    pillar_key = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pillar_name = Column(String, nullable=False)
    sub_pillar_name = Column(String, nullable=False)
    pillar_description = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint("pillar_name", "sub_pillar_name", name="uq_dim_pillars_name_sub_name"),
    )
