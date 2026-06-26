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
            activity_source_time_utc,
            hour,
            source_activity_offset,
            impression_count,
            click_through_count,
            redemption_count,
            in_store_redemption_count,
            rewards_activated_count,
            offer_bag_add_offer_count,
            offer_bag_remove_offer_count
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
            activity_source_time_utc,
            hour,
            source_activity_offset,
            impression_count,
            click_through_count,
            redemption_count,
            in_store_redemption_count,
            rewards_activated_count,
            offer_bag_add_offer_count,
            offer_bag_remove_offer_count FROM temp_mcd_offer_activities
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
            activity_source_time_utc=EXCLUDED.activity_source_time_utc,
            hour=EXCLUDED.hour,
            source_activity_offset=EXCLUDED.source_activity_offset,
            impression_count=EXCLUDED.impression_count,
            click_through_count=EXCLUDED.click_through_count,
            redemption_count=EXCLUDED.redemption_count,
            in_store_redemption_count=EXCLUDED.in_store_redemption_count,
            rewards_activated_count=EXCLUDED.rewards_activated_count,
            offer_bag_add_offer_count=EXCLUDED.offer_bag_add_offer_count,
            offer_bag_remove_offer_count=EXCLUDED.offer_bag_remove_offer_count;
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
        'hour',
        'source_activity_offset',
        'impression_count',
        'click_through_count',
        'redemption_count',
        'in_store_redemption_count',
        'rewards_activated_count',
        'offer_bag_add_offer_count',
        'offer_bag_remove_offer_count'
    ]
    batch=[]
    chunk_size=1000000
    total=0
    async for doc in cursor:
        row=(
            doc.get("activityid"),
            doc.get("activitydata"),
            to_int(doc.get("actiontypecode")),
            doc.get("actiontypename"),
            doc.get("devicetypecode"),
            doc.get("devicetypename"),
            to_int(doc.get("offerid")),
            doc.get("offername"),
            to_int(doc.get("venueid")),
            doc.get("market"),
            doc.get("reportingid"),
            to_datetime(doc.get("date")),
            to_datetime(doc.get("activitysourcetimelocal")),
            to_datetime(doc.get("activitysourcetimeutc")),
            doc.get("hour"),
            doc.get("sourceactivityoffset"),
            to_int(doc.get("impressioncount")),
            to_int(doc.get("clickthroughcount")),
            to_int(doc.get("redemptioncount")),
            to_int(doc.get("instoreredemptioncount")),
            to_int(doc.get("rewardsactivatedcount")),
            to_int(doc.get("offerbagaddoffercount")),
            to_int(doc.get("offerbagremoveoffercount"))
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
