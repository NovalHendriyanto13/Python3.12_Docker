import sys, os, asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool, to_uuid

TABLE_NAME="mcd_offer_activities"
TARGET_TABLE="mcd_offer_activities"

async def load_chunk_to_postgres(batch:list, columns:list):
    async with engine.begin() as conn:
        raw_conn=await conn.get_raw_connection()
        asyncpg_conn=raw_conn.driver_connection
        await conn.execute(text("CREATE TEMP TABLE temp_mcd_offer_activities (LIKE mcd_offer_activities EXCLUDING ALL) ON COMMIT DROP"))
        await asyncpg_conn.copy_records_to_table('temp_mcd_offer_activities', records=batch, columns=columns)
        upsert_query="""
        INSERT INTO mcd_offer_activities (
            activity_id,
            activity_data,
            action_type_code,
            action_type_name,
            device_type_code,
            device_type_name,
            offer_id,
            offer_name,
            venue_id,
            market,
            reporting_id,
            date,
            activity_source_time_local,
            activity_source_time_utc
        )
        SELECT 
            activity_id,
            activity_data,
            action_type_code,
            action_type_name,
            device_type_code,
            device_type_name,
            offer_id,
            offer_name,
            venue_id,
            market,
            reporting_id,
            date,
            activity_source_time_local,
            activity_source_time_utc
        FROM temp_mcd_offer_activities
        ON CONFLICT (activity_id) DO UPDATE SET
            activity_data=EXCLUDED.activity_data,
            action_type_code=EXCLUDED.action_type_code,
            action_type_name=EXCLUDED.action_type_name,
            device_type_code=EXCLUDED.device_type_code,
            device_type_name=EXCLUDED.device_type_name,
            offer_id=EXCLUDED.offer_id,
            offer_name=EXCLUDED.offer_name,
            venue_id=EXCLUDED.venue_id,
            market=EXCLUDED.market,
            reporting_id=EXCLUDED.reporting_id,
            date=EXCLUDED.date,
            activity_source_time_local=EXCLUDED.activity_source_time_local,
            activity_source_time_utc=EXCLUDED.activity_source_time_utc;
        """
        await conn.execute(text(upsert_query))

@task(retries=3,
        retry_delay_seconds=10,
        log_prints=True)
async def extract_and_load():
    client=AsyncIOMotorClient(mongo_uri)
    db=client[mongo_db]
    cursor=db["mcd_offer_activities"].find().batch_size(100000)
    columns=[
        'activity_id',
        'activity_data',
        'action_type_code',
        'action_type_name',
        'device_type_code',
        'device_type_name',
        'offer_id',
        'offer_name',
        'venue_id',
        'market',
        'reporting_id',
        'date',
        'activity_source_time_local',
        'activity_source_time_utc',
    ]
    batch=[]
    chunk_size=1000000
    total=0
    async for doc in cursor:
        row=(
            to_uuid(doc.get("activity_id")),
            doc.get("activity_data"),
            to_int(doc.get("action_type_code")),
            doc.get("action_type_name"),
            doc.get("device_type_code"),
            doc.get("device_type_name"),
            to_int(doc.get("offer_id")),
            doc.get("offer_name"),
            to_int(doc.get("venue_id")),
            doc.get("market"),
            to_uuid(doc.get("reporting_id")),
            to_datetime(doc.get("date")),
            to_datetime(doc.get("activity_source_time_local")),
            to_datetime(doc.get("activity_source_time_utc"))
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

@flow(name="offer_activities-etl")
async def etl_flow():
    await extract_and_load()

if __name__=="__main__":
    asyncio.run(etl_flow())
