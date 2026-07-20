import asyncio

from configs.database import Base, engine
import app.models

async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    print("Semua tabel berhasil dibuat.")

if __name__ == "__main__":
    print(Base.metadata.tables.keys())
    asyncio.run(create_tables())