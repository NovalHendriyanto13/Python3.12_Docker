import sys, os, asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool, to_uuid

TABLE_NAME="pushmessage_activities"
TARGET_TABLE="pushmessage_activities"

async def load_chunk_to_postgres(batch:list, columns:list):
    async with engine.begin() as conn:
        raw_conn=await conn.get_raw_connection()
        asyncpg_conn=raw_conn.driver_connection
        await conn.execute(text("CREATE TEMP TABLE temp_pushmessage_activities (LIKE pushmessage_activities EXCLUDING ALL) ON COMMIT DROP"))
        await asyncpg_conn.copy_records_to_table('temp_pushmessage_activities', records=batch, columns=columns)
        upsert_query="""
        INSERT INTO pushmessage_activities (activity_id,activity_data,action_type_code,action_type_name,device_type_code,device_type_name,message_id,message_name,num_messages,market,reporting_id,date,activity_source_time_local,activity_source_time_utc,hour,market_id,sent_count,beacon_triggers_count,geofence_trigger_count,seen_count,click_count)
        SELECT activity_id,activity_data,action_type_code,action_type_name,device_type_code,device_type_name,message_id,message_name,num_messages,market,reporting_id,date,activity_source_time_local,activity_source_time_utc,hour,market_id,sent_count,beacon_triggers_count,geofence_trigger_count,seen_count,click_count FROM temp_pushmessage_activities
        ON CONFLICT (activity_id) DO UPDATE SET
        activity_data=EXCLUDED.activity_data,action_type_code=EXCLUDED.action_type_code,action_type_name=EXCLUDED.action_type_name,device_type_code=EXCLUDED.device_type_code,device_type_name=EXCLUDED.device_type_name,message_id=EXCLUDED.message_id,message_name=EXCLUDED.message_name,num_messages=EXCLUDED.num_messages,market=EXCLUDED.market,reporting_id=EXCLUDED.reporting_id,date=EXCLUDED.date,activity_source_time_local=EXCLUDED.activity_source_time_local,activity_source_time_utc=EXCLUDED.activity_source_time_utc,hour=EXCLUDED.hour,market_id=EXCLUDED.market_id,sent_count=EXCLUDED.sent_count,beacon_triggers_count=EXCLUDED.beacon_triggers_count,geofence_trigger_count=EXCLUDED.geofence_trigger_count,seen_count=EXCLUDED.seen_count,click_count=EXCLUDED.click_count;
        """
        await conn.execute(text(upsert_query))

@task(retries=3,retry_delay_seconds=10,log_prints=True)
async def extract_and_load():
    client=AsyncIOMotorClient(mongo_uri)
    db=client[mongo_db]
    cursor=db["pushmessage_activities"].find().batch_size(100000)
    columns=['activity_id', 'activity_data', 'action_type_code', 'action_type_name', 'device_type_code', 'device_type_name', 'message_id', 'message_name', 'num_messages', 'market', 'reporting_id', 'date', 'activity_source_time_local', 'activity_source_time_utc', 'hour', 'market_id', 'sent_count', 'beacon_triggers_count', 'geofence_trigger_count', 'seen_count', 'click_count']
    batch=[]
    chunk_size=1000000
    total=0
    async for doc in cursor:
        row=(
            doc.get("activityid"),doc.get("activitydata"),to_int(doc.get("actiontypecode")),doc.get("actiontypename"),doc.get("devicetypecode"),doc.get("devicetypename"),to_int(doc.get("messageid")),doc.get("messagename"),to_int(doc.get("nummessages")),doc.get("market"),doc.get("reportingid"),to_datetime(doc.get("date")),to_datetime(doc.get("activitysourcetimelocal")),to_datetime(doc.get("activitysourcetimeutc")),doc.get("hour"),doc.get("marketid"),doc.get("sentcount"),to_int(doc.get("beacontriggerscount")),to_int(doc.get("geofencetriggercount")),to_int(doc.get("seencount")),to_int(doc.get("clickcount"))
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

@flow(name="pushmessage_activities-etl")
async def etl_flow():
    await extract_and_load()

if __name__=="__main__":
    asyncio.run(etl_flow())
