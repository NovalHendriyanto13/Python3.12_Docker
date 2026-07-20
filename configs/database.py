from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from configs import app_config
from contextlib import asynccontextmanager

engine = create_async_engine(app_config.database_url, echo=True)

AsyncSessionLocal = sessionmaker(
    bind= engine,
    class_= AsyncSession,
    expire_on_commit= False,
)

class Base(DeclarativeBase):
    pass

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

@asynccontextmanager
async def get_session():
    async with AsyncSessionLocal() as session:
        yield session