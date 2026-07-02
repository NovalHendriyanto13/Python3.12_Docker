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

tablename = "advertisement_activities"
past_date = date.today() -  timedelta(days=timedelta_days)

@task(retries=3, retry_delay_seconds=10, log_prints=True, name="fetch_advertisement_activities")
async def fetch_data_task():
    logger = get_run_logger()
    logger.info(f"Fetching data from advertisement_activities")

    columns = [
        "activity_id",
        "action_type_code",
        "action_type_name",
        "advertisement_id",
        "advertisement_name",
        "device_type_code",
        "device_type_name",
        "market",
        "reporting_id",
        "date",
        "activity_source_time_local",
        "activity_source_time_utc"
    ]

    rows = await run_in_threadpool(
        databricks_fetch_data,
        tablename, 
        columns = columns,
        criteria = {
            "activity_source_time_utc": {"op": ">=", "value": past_date}
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

    result = await upsert_mongo(tablename, rows, unique_key="activity_id")

    logger.info(f"Inserted {result} records in collection {tablename}")
    return result

@flow(name="databricks_to_mongo_advertisement_activities_sync", log_prints=True)
async def databricks_to_mongo_advertisement_activities_sync():
    logger = get_run_logger()
    logger.info("=== Sync flow started ===")

    await connect_to_mongo()

    try:
        rows = await fetch_data_task()
        upserted_count = await insert_to_mongo_task(rows)

        logger.info("=== Sync flow completed ===")
        return {"status": "success", "upsert": upserted_count}

    except Exception as e:
        logger.error(f"Sync flow failed: {e}")
        raise

    finally:
        await close_mongo_connection()

if __name__ == "__main__":
    asyncio.run(databricks_to_mongo_advertisement_activities_sync())