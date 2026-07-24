import sys
import os
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool, to_uuid
from configs.constants import device_types
import uuid

# Helper function to load a single chunk to PostgreSQL using COPY + Merge
async def load_chunk_to_postgres(batch: list, columns: list):
    async with engine.begin() as conn:
        raw_conn = await conn.get_raw_connection()
        asyncpg_conn = raw_conn.driver_connection
        
        # Create staging table for this chunk
        await conn.execute(
            text("CREATE TEMP TABLE temp_mcd_advertisement_activities (LIKE mcd_advertisement_activities EXCLUDING ALL) ON COMMIT DROP")
        )
        
        # Stream load to staging table
        await asyncpg_conn.copy_records_to_table(
            'temp_mcd_advertisement_activities', records=batch, columns=columns
        )
        
        # Merge/Upsert query
        upsert_query = """
        INSERT INTO mcd_advertisement_activities (
            activity_id,
            action_type_code,
            action_type_name,
            advertisement_id,
            advertisement_name,
            device_type_code,
            device_type_name,
            market,
            reporting_id,
            date,
            activity_source_time_local,
            activity_source_time_utc
        )
        SELECT 
            activity_id,
            action_type_code,
            action_type_name,
            advertisement_id,
            advertisement_name,
            device_type_code,
            device_type_name,
            market,
            reporting_id,
            date,
            activity_source_time_local,
            activity_source_time_utc
        FROM temp_mcd_advertisement_activities
        ON CONFLICT (activity_id) DO UPDATE SET
            action_type_code = EXCLUDED.action_type_code,
            action_type_name = EXCLUDED.action_type_name,
            advertisement_id = EXCLUDED.advertisement_id,
            advertisement_name = EXCLUDED.advertisement_name,
            device_type_code = EXCLUDED.device_type_code,
            device_type_name = EXCLUDED.device_type_name,
            market = EXCLUDED.market,
            reporting_id = EXCLUDED.reporting_id,
            date = EXCLUDED.date,
            activity_source_time_local = EXCLUDED.activity_source_time_local,
            activity_source_time_utc = EXCLUDED.activity_source_time_utc;
        """
        await conn.execute(text(upsert_query))

@task(retries=3, retry_delay_seconds=10, log_prints=True)
async def extract_and_load_advertisement_activities_fast():
    client = AsyncIOMotorClient(
        mongo_uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000, 
    )
    db = client[mongo_db]
    
    # Stream from MongoDB using cursor
    cursor = db["mcd_advertisement_activities"].find().batch_size(100000)
    
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
    
    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0
    
    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")
    
    async for doc in cursor:
        row = (
            to_uuid(doc.get("activity_id")),
            to_int(doc.get("action_type_code")),
            (doc.get("action_type_name")),
            to_int(doc.get("advertisementid")),
            (doc.get("advertisementname")),
            to_int(doc.get("devicetype")),
            device_types[to_int(doc.get("devicetype"))],
            (doc.get("market")),
            to_uuid(doc.get("reportingid")),
            to_datetime(doc.get("date")),
            to_datetime(doc.get("activity_source_time_local")),
            to_datetime(doc.get("sourceactivitytimeutc"))
        )

        batch.append(row)
        
        if len(batch) >= chunk_size:
            await load_chunk_to_postgres(batch, columns)
            total += len(batch)
            print(f"   ✅ Synchronized chunk of {len(batch)} advertisement activities (Total: {total})")
            batch = []
            # Beri jeda ke event loop agar Prefect bisa mengirim heartbeat
            await asyncio.sleep(0.1)
            
    if batch:
        await load_chunk_to_postgres(batch, columns)
        total += len(batch)
        print(f"   ✅ Synchronized chunk of {len(batch)} advertisement activities (Total: {total})")
        
    print(f"🚀 Bulk Sync Finished! Total {total} advertisement activities successfully upserted.")

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_advertisement_activities_flow():
    print("ETL Flow advertisement activities started...")
    await extract_and_load_advertisement_activities_fast()
    print("ETL Flow advertisement activities finished!")

if __name__ == "__main__":
    asyncio.run(etl_advertisement_activities_flow())