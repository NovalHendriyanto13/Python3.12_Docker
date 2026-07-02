import sys, os, asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task, get_run_logger
from fastapi.concurrency import run_in_threadpool
from datetime import date, timedelta
from configs.databricks import databricks_fetch_data
from configs.mongo import connect_to_mongo, close_mongo_connection, upsert_mongo
from configs.app_config import timedelta_days

tablename = "offers"
past_date = date.today() -  timedelta(days=timedelta_days)

@task(retries=3, retry_delay_seconds=10, log_prints=True, name="fetch_offers")
async def fetch_data_task():
    logger = get_run_logger()
    logger.info(f"Fetching data from offers")

    columns=[
        'offer_id',
        'campaign_id',
        'category_id',
        'category',
        'market',
        'title',
        'description',
        'terms_and_conditions',
        'has_barcode_image',
        'redemption_limit',
        'payment_type',
        'redemption_count_unlimited',
        'code_type',
        'discount_percent',
        'discount_value',
        'status',
        'apply_geo_fence_filters',
        'base_weight',
        'no_compete_group',
        'is_giftable',
        'is_reward',
        'is_respawning',
        'respawns_in_days',
        'enable_distance_weight',
        'is_available_all_stores',
        'promotional_image_description',
        'limit',
        'code_expiry_in_minutes',
        'is_sticky',
        'sticky_expiration_days',
        'sticky_expiration_time_of_day',
        'offer_type',
        'respawn_start_time',
        'name',
        'respawns_in_minutes',
        'consumer_redemption_limit',
        'redemption_text',
        'days_of_week',
        'daily_start_time',
        'daily_end_time',
        'offer_start_time',
        'offer_expire_time',
        'when_last_updated_utc'
    ]

    rows = await run_in_threadpool(
        databricks_fetch_data,
        tablename, 
        columns = columns,
        criteria = {
            "when_last_updated_utc": {"op": ">=", "value": past_date}
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

    result = await upsert_mongo(tablename, rows, unique_key="offer_id")

    logger.info(f"Inserted {result} records in collection {tablename}")
    return result

@flow(name="databricks_to_mongo_offers_sync", log_prints=True)
async def databricks_to_mongo_offers_sync():
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
    asyncio.run(databricks_to_mongo_offers_sync())
