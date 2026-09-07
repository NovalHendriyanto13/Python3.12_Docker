from sqlalchemy import Column, String, Index, UniqueConstraint
from configs.database import Base
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
import uuid

class DimProducts(Base):
    __tablename__ = "dim_products"

    product_key = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    product_id = Column(String, nullable=False)
    product_name = Column(String, nullable=False)
    main_category = Column(String, nullable=True)
    main_sub_menu = Column(String, nullable=True)
    sub_category = Column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint("product_name", name="uq_dim_products_product_name"),
        Index("ix_product_main_category", "main_category"),
    )