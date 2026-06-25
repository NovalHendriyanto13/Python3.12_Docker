import sys, os, asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool, to_uuid

TABLE_NAME="mcd_loyalty_activities"
TARGET_TABLE="mcd_loyalty_activities"

async def load_chunk_to_postgres(batch:list, columns:list):
    async with engine.begin() as conn:
        raw_conn=await conn.get_raw_connection()
        asyncpg_conn=raw_conn.driver_connection
        await conn.execute(text("CREATE TEMP TABLE temp_mcd_loyalty_activities (LIKE mcd_loyalty_activities EXCLUDING ALL) ON COMMIT DROP"))
        await asyncpg_conn.copy_records_to_table('temp_mcd_loyalty_activities', records=batch, columns=columns)
        upsert_query="""
        INSERT INTO mcd_loyalty_activities (
            reporting_id,
            venue_id,
            activated_reward_id,
            activity_id,
            loyalty_program_id,
            market,
            action_type_code,
            action_type_name,
            activated_reward_name,
            activity_data,
            device_type_code,
            device_type_name,
            loyalty_program_name,
            num_points,
            date,
            activity_source_time_local,
            activity_source_time_utc
        )
        SELECT 
            reporting_id,
            venue_id,
            activated_reward_id,
            activity_id,
            loyalty_program_id,
            market,
            action_type_code,
            action_type_name,
            activated_reward_name,
            activity_data,
            device_type_code,
            device_type_name,
            loyalty_program_name,
            num_points,
            date,
            activity_source_time_local,
            activity_source_time_utc 
        FROM temp_loyalty_activities
        ON CONFLICT (activity_id) DO UPDATE SET
            venue_id=EXCLUDED.venue_id,
            activated_reward_id=EXCLUDED.activated_reward_id,
            activity_id=EXCLUDED.activity_id,
            loyalty_program_id=EXCLUDED.loyalty_program_id,
            market=EXCLUDED.market,
            action_type_code=EXCLUDED.action_type_code,
            action_type_name=EXCLUDED.action_type_name,
            activated_reward_name=EXCLUDED.activated_reward_name,
            activity_data=EXCLUDED.activity_data,
            device_type_code=EXCLUDED.device_type_code,
            device_type_name=EXCLUDED.device_type_name,
            loyalty_program_name=EXCLUDED.loyalty_program_name,
            num_points=EXCLUDED.num_points,
            date=EXCLUDED.date,
            activity_source_time_local=EXCLUDED.activity_source_time_local,
            activity_source_time_utc=EXCLUDED.activity_source_time_utc;
        """
        await conn.execute(text(upsert_query))

@task(retries=3,retry_delay_seconds=10,log_prints=True)
async def extract_and_load():
    client=AsyncIOMotorClient(mongo_uri)
    db=client[mongo_db]
    cursor=db["mcd_loyalty_activities"].find().batch_size(100000)
    columns=[
        'reporting_id', 
        'venue_id', 
        'activated_reward_id', 
        'activity_id', 
        'loyalty_program_id', 
        'market', 
        'action_type_code', 
        'action_type_name', 
        'activated_reward_name', 
        'activity_data', 
        'device_type_code', 
        'device_type_name', 
        'loyalty_program_name', 
        'num_points', 
        'date', 
        'activity_source_time_local', 
        'activity_source_time_utc'
    ]
    batch=[]
    chunk_size=1000000
    total=0
    async for doc in cursor:
        row=(
            doc.get("reportingid"),
            to_int(doc.get("venueid")),
            to_int(doc.get("activatedrewardid")),
            doc.get("activityid"),
            to_int(doc.get("loyaltyprogramid")),
            doc.get("market"),
            to_int(doc.get("actiontypecode")),
            doc.get("actiontypename"),
            doc.get("activatedrewardname"),
            doc.get("activitydata"),
            doc.get("devicetypecode"),
            doc.get("devicetypename"),
            doc.get("loyaltyprogramname"),
            to_int(doc.get("numpoints")),
            to_datetime(doc.get("date")),
            to_datetime(doc.get("activitysourcetimelocal")),
            to_datetime(doc.get("activitysourcetimeutc"))
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

@flow(name="loyalty_activities-etl")
async def etl_flow():
    await extract_and_load()

if __name__=="__main__":
    asyncio.run(etl_flow())
