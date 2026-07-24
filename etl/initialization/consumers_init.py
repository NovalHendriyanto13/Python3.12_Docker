import sys
import os
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import Depends
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from sqlalchemy.ext.asyncio import AsyncSession
from configs.database import get_session
from app.services.mongo.consumers_mongo import consumers_init

@task(retries=3, retry_delay_seconds=10, log_prints=True, cache_policy=NO_CACHE)
async def extract_and_load_consumer(db: AsyncSession):
    try:
        await consumers_init(db)
        
        await db.commit()
        print(f"🚀 Bulk Sync Finished! the process successfully upserted.")
    except Exception:
        await db.rollback()
        raise

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_consumers_flow():
    print("ETL Flow consumers started...")
    async with get_session() as db:
        await extract_and_load_consumer(db)

    print("ETL Flow consumers finished!")

if __name__ == "__main__":
    asyncio.run(etl_consumers_flow())