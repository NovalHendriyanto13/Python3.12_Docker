import sys
import os
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import Depends
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from sqlalchemy.ext.asyncio import AsyncSession
from configs.database import get_session
from app.services.mongo.visit_mongo import visit_summary
from app.helpers.app_helper import get_date_range

@task(retries=1, retry_delay_seconds=10, log_prints=True, cache_policy=NO_CACHE)
async def extract_and_load_visit(db: AsyncSession):
    try:
        start_date, end_date = get_date_range('2023-01-01', '2026-06-30')
        await visit_summary(db, start_date, end_date)
        
        await db.commit()
        print(f"🚀 Bulk Sync Finished! the process successfully upserted.")
    except Exception:
        await db.rollback()
        raise

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_visit_flow():
    print("ETL Flow visit started...")
    async with get_session() as db:
        await extract_and_load_visit(db)

    print("ETL Flow visit finished!")

if __name__ == "__main__":
    asyncio.run(etl_visit_flow())