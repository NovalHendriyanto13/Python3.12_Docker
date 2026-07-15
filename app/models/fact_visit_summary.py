from sqlalchemy import Column, String, Integer
from configs.database import Base

class FactVisitSummary(Base):
    __tablename__ = "fact_visit_summary"
    
    visit_summary_key = Column(Integer, primary_key=True, autoincrement=True)
    date_key = Column(Integer, nullable=False)
    venue_key = Column(String, nullable=True)
    total_visit = Column(Numeric(10, 0))
    market = Column(String, nullable=True)
