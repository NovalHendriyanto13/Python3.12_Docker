from sqlalchemy import Column, UniqueConstraint, Index
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID
import uuid

class DimProductPillars(Base):
    __tablename__ = "dim_product_pillars"

    product_pillar_key = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pillar_key = Column(UUID(as_uuid=True), nullable=False)
    product_key = Column(UUID(as_uuid=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("product_key", "pillar_key", name="uq_dim_product_pillars_product_pillar"),
        Index("ix_product_pillar_pillar_id", "pillar_key"),
        Index("ix_product_pillar_product_id", "product_key"),
    )
