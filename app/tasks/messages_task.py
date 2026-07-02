import sys, os, asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task, get_run_logger
from fastapi.concurrency import run_in_threadpool
from datetime import date, timedelta
from configs.databricks import databricks_fetch_data
from configs.mongo import connect_to_mongo, close_mongo_connection, upsert_mongo
from configs.app_config import timedelta_days

tablename = "messages"
past_date = date.today() -  timedelta(days=timedelta_days)

@task(retries=3, retry_delay_seconds=10, log_prints=True, name="fetch_messages")
async def fetch_data_task():
    logger = get_run_logger()
    logger.info(f"Fetching data from messages")

    columns=[
        'message_id', 
        'market', 
        'status', 
        'trigger_type_code', 
        'name', 
        'channel_type', 
        'campaign_id', 
        'subject', 
        'body', 
        'email_text_body', 
        'offer_id', 
        'recurring_type_code', 
        'daily_start_time', 
        'daily_end_time', 
        'time_frame_type', 
        'date_modified', 
        'date_created', 
        'time_frame_start_date', 
        'time_frame_end_date'
    ]

    rows = await run_in_threadpool(
        databricks_fetch_data,
        tablename, 
        columns = columns,
        criteria = {
            "date_modified": {"op": ">=", "value": past_date}
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

    result = await upsert_mongo(tablename, rows, unique_key="message_id")

    logger.info(f"Inserted {result} records in collection {tablename}")
    return result

@flow(name="databricks_to_mongo_messages_sync", log_prints=True)
async def databricks_to_mongo_messages_sync():
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
    asyncio.run(databricks_to_mongo_messages_sync())

async def load_chunk_to_postgres(batch:list, columns:list):
    async with engine.begin() as conn:
        raw_conn=await conn.get_raw_connection()
        asyncpg_conn=raw_conn.driver_connection
        await conn.execute(text("CREATE TEMP TABLE temp_mcd_messages (LIKE messages EXCLUDING ALL) ON COMMIT DROP"))
        await asyncpg_conn.copy_records_to_table('temp_mcd_messages', records=batch, columns=columns)
        upsert_query="""
        INSERT INTO mcd_messages (
            message_id,
            market,
            status,
            trigger_type_code,
            name,
            channel_type,
            campaign_id,
            subject,
            body,
            email_text_body,
            offer_id,
            recurring_type_code,
            daily_start_time,
            daily_end_time,
            time_frame_type,
            date_modified,
            date_created,
            time_frame_start_date,
            time_frame_end_date
        )
        SELECT 
            message_id,
            market,
            status,
            trigger_type_code,
            name,
            channel_type,
            campaign_id,
            subject,
            body,
            email_text_body,
            offer_id,
            recurring_type_code,
            daily_start_time,
            daily_end_time,
            time_frame_type,
            date_modified,
            date_created,
            time_frame_start_date,
            time_frame_end_date FROM temp_mcd_messages
        ON CONFLICT (message_id) DO UPDATE SET
            market=EXCLUDED.market,
            status=EXCLUDED.status,
            trigger_type_code=EXCLUDED.trigger_type_code,
            name=EXCLUDED.name,
            channel_type=EXCLUDED.channel_type,
            campaign_id=EXCLUDED.campaign_id,
            subject=EXCLUDED.subject,
            body=EXCLUDED.body,
            email_text_body=EXCLUDED.email_text_body,
            offer_id=EXCLUDED.offer_id,
            recurring_type_code=EXCLUDED.recurring_type_code,
            daily_start_time=EXCLUDED.daily_start_time,
            daily_end_time=EXCLUDED.daily_end_time,
            time_frame_type=EXCLUDED.time_frame_type,
            date_modified=EXCLUDED.date_modified,
            date_created=EXCLUDED.date_created,
            time_frame_start_date=EXCLUDED.time_frame_start_date,
            time_frame_end_date=EXCLUDED.time_frame_end_date;
        """
        await conn.execute(text(upsert_query))

@task(retries=3,retry_delay_seconds=10,log_prints=True)
async def extract_and_load():
    client=AsyncIOMotorClient(mongo_uri)
    db=client[mongo_db]
    cursor=db["mcd_messages"].find().batch_size(100000)
    
    batch=[]
    chunk_size=1000000
    total=0
    async for doc in cursor:
        row=(
            to_int(doc.get("message_id")),
            doc.get("market"),
            to_int(doc.get("status")),
            to_int(doc.get("trigger_type_code")),
            doc.get("name"),
            to_int(doc.get("channel_type")),
            to_int(doc.get("campaign_id")),
            doc.get("subject"),
            doc.get("body"),
            doc.get("email_text_body"),
            to_int(doc.get("offer_id")),
            to_int(doc.get("recurring_type_code")),
            to_int(doc.get("daily_start_time")),
            to_int(doc.get("daily_end_time")),
            to_int(doc.get("time_frame_type")),
            to_datetime(doc.get("date_modified")),
            to_datetime(doc.get("date_created")),
            to_datetime(doc.get("time_frame_start_date")),
            to_datetime(doc.get("time_frame_end_date"))
        )
        batch.append(row)
        if len(batch)>=chunk_size:
            await load_chunk_to_postgres(batch,columns)
            total+=len(batch)
            batch=[]
            await asyncio.sleep(0.1)
    if batch:
        await load_chunk_to_postgres(batch,columns)
        total+=len(batch)
    print(f"Finished sync {total} rows")

@flow(name="messages-etl")
async def etl_messages_flow():
    await extract_and_load()

if __name__=="__main__":
    asyncio.run(etl_messages_flow())
