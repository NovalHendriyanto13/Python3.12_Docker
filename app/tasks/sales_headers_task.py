import sys
import os
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task, get_run_logger
from fastapi.concurrency import run_in_threadpool
from datetime import date, timedelta
from configs.databricks import databricks_fetch_data
from configs.mongo import connect_to_mongo, close_mongo_connection, upsert_mongo
from configs.app_config import timedelta_days

tablename = "sales_headers"
past_date = date.today() -  timedelta(days=timedelta_days)

@task(retries=3, retry_delay_seconds=10, log_prints=True, name="fetch_sales_headers")
async def fetch_data_task():
    logger = get_run_logger()
    logger.info(f"Fetching data from sales_headers")

    columns = [
        "sale_id",
        "pos_transaction_id",
        "reporting_id",
        "offer_ids",
        "venue_external_id",
        "total_amount",
        "tax_total_amount",
        "gross_amount",
        "before_discount_tax_total_amount",
        "before_discount_total_amount",
        "day_part",
        "pod_type",
        "transaction_kind",
        "order_take_platform",
        "sale_type",
        "date",
        "market",
        "transaction_source_time_local",
        "plexure_processing_time_utc"
    ]

    rows = await run_in_threadpool(
        databricks_fetch_data,
        tablename, 
        columns = columns,
        criteria = {
            "transaction_source_time_local": {"op": ">=", "value": past_date}
        }
    )
    
    logger.info(f"Fetched {len(rows)} rows from Databricks")
    print(f"Fetched {len(rows)} rows from Databricks")
    return rows

@task(retries=2, retry_delay_seconds=10, name="insert_to_mongo")
async def insert_to_mongo_task(rows: list):
    logger = get_run_logger()

    if not rows:
        logger.warning("No data to insert, skipping")
        return 0

    result = await upsert_mongo(tablename, rows, unique_key="sale_id")

    logger.info(f"Inserted {result} records in collection {tablename}")
    return result

@flow(name="databricks_to_mongo_sales_headers_sync", log_prints=True)
async def databricks_to_mongo_sales_headers_sync():
    logger = get_run_logger()
    logger.info("=== Sync flow started ===")

    await connect_to_mongo()

    try:
        rows = await fetch_data_task()
        upserted_count = await insert_to_mongo_task(rows)

        logger.info("=== Sync flow completed ===")
        return {"status": "success", "inserted": upserted_count}

    except Exception as e:
        logger.error(f"Sync flow failed: {e}")
        raise

    finally:
        await close_mongo_connection()

if __name__ == "__main__":
    asyncio.run(databricks_to_mongo_sales_headers_sync())
