import sys, os, asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool, to_uuid

TABLE_NAME="mcd_messages"
TARGET_TABLE="mcd_messages"

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
