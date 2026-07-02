import sys, os, asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task, get_run_logger
from fastapi.concurrency import run_in_threadpool
from datetime import date, timedelta
from configs.databricks import databricks_fetch_data
from configs.mongo import connect_to_mongo, close_mongo_connection, upsert_mongo
from configs.app_config import timedelta_days

tablename = "loyalty_programs"
past_date = date.today() -  timedelta(days=timedelta_days)

@task(retries=3, retry_delay_seconds=10, log_prints=True, name="fetch_loyalty_programs")
async def fetch_data_task():
    logger = get_run_logger()
    logger.info(f"Fetching data from loyalty_programs")

    columns=[
        'loyalty_program_id', 
        'campaign_id', 
        'category_id', 
        'max_instances', 
        'extended_data', 
        'name', 
        'title', 
        'sub_title', 
        'description', 
        'instructions', 
        'status', 
        'terms_and_conditions', 
        'points_required', 
        'days_of_week', 
        'weighting', 
        'daily_start_time', 
        'daily_end_time', 
        'max_points_per_day', 
        'apply_initial_points_to_subsequent_cards', 
        'max_points_requests_per_day', 
        'initial_points', 
        'is_hidden', 
        'require_ip_whitelisting', 
        'loyalty_program_type', 
        'points_expiry_days', 
        'expiry_schedule_details', 
        'is_consumer_api_write_accessible', 
        'market', 
        'start_date', 
        'end_date', 
        'when_last_updated'
    ]

    rows = await run_in_threadpool(
        databricks_fetch_data,
        tablename, 
        columns = columns,
        criteria = {
            "when_last_updated": {"op": ">=", "value": past_date}
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

    result = await upsert_mongo(tablename, rows, unique_key="loyalty_program_id")

    logger.info(f"Inserted {result} records in collection {tablename}")
    return result

@flow(name="databricks_to_mongo_loyalty_programs_sync", log_prints=True)
async def databricks_to_mongo_loyalty_programs_sync():
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
    asyncio.run(databricks_to_mongo_loyalty_programs_sync())
