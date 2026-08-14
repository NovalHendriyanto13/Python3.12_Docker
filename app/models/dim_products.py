from sqlalchemy import Column, String, Index
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class DimProducts(Base):
    __tablename__ = "dim_products"
    
    product_key = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    product_pillar_key = Column(UUID(as_uuid=True))
    product_id = Column(String, nullable=False)
    product_name = Column(String, nullable=False)
    main_category = Column(String, nullable=False)
    main_sub_menu = Column(String, nullable=False)
    sub_category = Column(String, nullable=False)

    __table_args__ = (
        Index("ix_product_id", "product_id"),
        Index("ix_dim_products_product_pillar_key", "product_pillar_key"),
    )