import sys
import os
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool

# Helper function to load a single chunk to PostgreSQL using COPY + Merge
async def load_chunk_to_postgres(batch: list, columns: list):
    async with engine.begin() as conn:
        raw_conn = await conn.get_raw_connection()
        asyncpg_conn = raw_conn.driver_connection
        
        # Create staging table for this chunk
        await conn.execute(
            text("CREATE TEMP TABLE temp_mcd_advertisements (LIKE mcd_advertisements EXCLUDING ALL) ON COMMIT DROP")
        )
        
        # Stream load to staging table
        await asyncpg_conn.copy_records_to_table(
            'temp_mcd_advertisements', records=batch, columns=columns
        )
        
        # Merge/Upsert query
        upsert_query = """
        INSERT INTO mcd_advertisements (
            advertisement_id,
            campaign_id,
            market,
            name,
            title,
            description,
            click_through_url,
            status,
            channel_code,
            placement_code,
            no_compete_group,
            enable_time_based_weight,
            enable_distance_weight,
            apply_geo_fence_filters,
            apply_tag_value_filter,
            weight,
            days_of_week,
            daily_start_time,
            daily_end_time,
            date_modified,
            date_created,
            start_date,
            end_date
        )
        SELECT 
            advertisement_id,
            campaign_id,
            market,
            name,
            title,
            description,
            click_through_url,
            status,
            channel_code,
            placement_code,
            no_compete_group,
            enable_time_based_weight,
            enable_distance_weight,
            apply_geo_fence_filters,
            apply_tag_value_filter,
            weight,
            days_of_week,
            daily_start_time,
            daily_end_time,
            date_modified,
            date_created,
            start_date,
            end_date 
        FROM temp_mcd_advertisements
        ON CONFLICT (advertisement_id) DO UPDATE SET
            campaign_id = EXCLUDED.campaign_id,
            market = EXCLUDED.market,
            name = EXCLUDED.name,
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            click_through_url = EXCLUDED.click_through_url,
            status = EXCLUDED.status,
            channel_code = EXCLUDED.channel_code,
            placement_code = EXCLUDED.placement_code,
            no_compete_group = EXCLUDED.no_compete_group,
            enable_time_based_weight = EXCLUDED.enable_time_based_weight,
            enable_distance_weight = EXCLUDED.enable_distance_weight,
            apply_geo_fence_filters = EXCLUDED.apply_geo_fence_filters,
            apply_tag_value_filter = EXCLUDED.apply_tag_value_filter,
            weight = EXCLUDED.weight,
            days_of_week = EXCLUDED.days_of_week,
            daily_start_time = EXCLUDED.daily_start_time,
            daily_end_time = EXCLUDED.daily_end_time,
            date_modified = EXCLUDED.date_modified,
            date_created = EXCLUDED.date_created,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date;
        """
        await conn.execute(text(upsert_query))

@task(retries=3, retry_delay_seconds=10, log_prints=True)
async def extract_and_load_advertisements_fast():
    client = AsyncIOMotorClient(
        mongo_uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000, 
    )
    db = client[mongo_db]
    
    # Stream from MongoDB using cursor
    cursor = db["mcd_advertisements"].find().batch_size(100000)
    
    columns = [
        "advertisement_id",
        "campaign_id",
        "market",
        "name",
        "title",
        "description",
        "click_through_url",
        "status",
        "channel_code",
        "placement_code",
        "no_compete_group",
        "enable_time_based_weight",
        "enable_distance_weight",
        "apply_geo_fence_filters",
        "apply_tag_value_filter",
        "weight",
        "days_of_week",
        "daily_start_time",
        "daily_end_time",
        "date_modified",
        "date_created",
        "start_date",
        "end_date"
    ]
    
    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0
    
    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")
    
    async for doc in cursor:
        row = (
            to_int(doc.get("id")),
            to_int(doc.get("campaignid")),
            doc.get("market"),
            doc.get("name"),
            doc.get("title"),
            doc.get("description"),
            doc.get("click_through_url"),
            to_int(doc.get("status")),
            doc.get("channel_code"),
            doc.get("placement_code"),
            doc.get("no_compete_group"),
            to_bool(doc.get("enable_time_based_weight")),
            to_bool(doc.get("enable_distance_weight")),
            to_bool(doc.get("apply_geo_fence_filters")),
            to_bool(doc.get("apply_tag_value_filter")),
            to_int(doc.get("weight")),
            doc.get("days_of_week"),
            to_int(doc.get("daily_start_time")),
            to_int(doc.get("daily_end_time")),
            to_datetime(doc.get("date_modified")),
            to_datetime(doc.get("date_created")),
            to_datetime(doc.get("startdate")),
            to_datetime(doc.get("enddate"))
        )
        batch.append(row)
        
        if len(batch) >= chunk_size:
            await load_chunk_to_postgres(batch, columns)
            total += len(batch)
            print(f"   ✅ Synchronized chunk of {len(batch)} advertisements (Total: {total})")
            batch = []
            # Beri jeda ke event loop agar Prefect bisa mengirim heartbeat
            await asyncio.sleep(0.1)
            
    if batch:
        await load_chunk_to_postgres(batch, columns)
        total += len(batch)
        print(f"   ✅ Synchronized chunk of {len(batch)} advertisements (Total: {total})")
        
    print(f"🚀 Bulk Sync Finished! Total {total} advertisements successfully upserted.")

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_advertisements_flow():
    print("ETL Flow advertisement started...")
    await extract_and_load_advertisements_fast()
    print("ETL Flow advertisement finished!")

if __name__ == "__main__":
    asyncio.run(etl_advertisements_flow())