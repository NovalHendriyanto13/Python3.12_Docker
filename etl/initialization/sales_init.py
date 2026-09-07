import sys
import os
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import Depends
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from sqlalchemy.ext.asyncio import AsyncSession
from configs.database import get_session
from app.services.mongo.sales_mongo import sales_summary, consumer_sales_summary, \
    consumer_sales_header_summary, consumer_sales_hourly_summary
from app.helpers.app_helper import chunk_month_range
import time

@task(retries=1, retry_delay_seconds=10, log_prints=True, cache_policy=NO_CACHE)
async def extract_and_load_sales_summary(db: AsyncSession, start_date, end_date):
    try:
        await sales_summary(db, start_date, end_date)
        await db.commit()
        print(f"🚀 Bulk Sync Finished! the process Sales Summary successfully upserted.")
    except Exception:
        await db.rollback()
        raise

@task(retries=1, retry_delay_seconds=10, log_prints=True, cache_policy=NO_CACHE)
async def extract_and_load_consumer_sales_summary(db: AsyncSession, start_date, end_date):
    try:
        await consumer_sales_summary(db, start_date, end_date)
        
        await db.commit()
        print(f"🚀 Bulk Sync Finished! the process Consumer Sales successfully upserted.")
    except Exception:
        await db.rollback()
        raise

@task(retries=1, retry_delay_seconds=10, log_prints=True, cache_policy=NO_CACHE)
async def extract_and_load_consumer_sales_header(db: AsyncSession, start_date, end_date):
    try:
        await consumer_sales_header_summary(db, start_date, end_date)

        await db.commit()
        print(f"🚀 Bulk Sync Finished! the process Consumer Sales Header successfully upserted.")
    except Exception:
        await db.rollback()
        raise

@task(retries=1, retry_delay_seconds=10, log_prints=True, cache_policy=NO_CACHE)
async def extract_and_load_consumer_sales_hourly(db: AsyncSession, start_date, end_date):
    try:
        await consumer_sales_hourly_summary(db, start_date, end_date)

        await db.commit()
        print(f"🚀 Bulk Sync Finished! the process Consumer Sales Hourly successfully upserted.")
    except Exception:
        await db.rollback()
        raise

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_sales_flow():
    print("ETL Flow sales started...")
    async with get_session() as db:
        for start_date, end_date in chunk_month_range('2023-01-01', '2026-06-30'):
            print(f"Processing sales range {start_date.date()} - {end_date.date()}...")

            # await extract_and_load_sales_summary(db, start_date, end_date)
            # time.sleep(5)

            # await extract_and_load_consumer_sales_summary(db, start_date, end_date)
            # time.sleep(5)

            await extract_and_load_consumer_sales_header(db, start_date, end_date)
            time.sleep(5)

            await extract_and_load_consumer_sales_hourly(db, start_date, end_date)
            time.sleep(5)

    print("ETL Flow sales finished!")

if __name__ == "__main__":
    asyncio.run(etl_sales_flow())