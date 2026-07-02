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

tablename = "stamp_program_transactions"
past_date = date.today() -  timedelta(days=timedelta_days)

@task(retries=3, retry_delay_seconds=10, log_prints=True, name="fetch_stamp_program_transactions")
async def fetch_data_task():
    logger = get_run_logger()
    logger.info(f"Fetching data from stamp_program_transactions")

    columns = [
        "stamp_program_transaction_id",
        "pos_sales_transaction_id",
        "sale_id",
        "incentive_program_id",
        "incentive_program_name",
        "transaction_origin",
        "is_initial_rewarded_stamps",
        "note",
        "stamp_card_id",
        "total_card_stamps_after_transaction",
        "number_of_stamps_added",
        "external_transaction_reference",
        "venue_id",
        "venue_name",
        "venue_external_id",
        "market",
        "reporting_id",
        "date",
        "transaction_source_time_local",
        "transaction_time_utc"
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

    result = await upsert_mongo(tablename, rows, unique_key="stamp_program_transaction_id")

    logger.info(f"Inserted {result} records in collection {tablename}")
    return result

@flow(name="databricks_to_mongo_stamp_program_transactions_sync", log_prints=True)
async def databricks_to_mongo_stamp_program_transactions_sync():
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
    asyncio.run(databricks_to_mongo_stamp_program_transactions_sync())
