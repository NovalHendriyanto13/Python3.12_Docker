from sqlalchemy import Column, String, Date, Boolean, Numeric, Integer
from configs.database import Base

class FactSalesSummary(Base):
    __tablename__ = "fact_sales_summary"
    
    sales_summary_key = Column(Integer, primary_key=True, autoincrement=True)
    date_key = Column(Integer, unique=True, nullable=True)
    total_amount = Column(Numeric(10, 0))
    gross_amount = Column(Numeric(10, 0))
    tax_total_amount = Column(Numeric(10, 0))
    before_discount_tax_total_amount = Column(Numeric(10, 0))
    before_discount_total_amount = Column(Numeric(10, 0))