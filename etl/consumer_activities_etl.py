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
from configs.constants import campaign_status
import uuid

# Helper function to load a single chunk to PostgreSQL using COPY + Merge
async def load_chunk_to_postgres(batch: list, columns: list):
    async with engine.begin() as conn:
        raw_conn = await conn.get_raw_connection()
        asyncpg_conn = raw_conn.driver_connection
        
        # Create staging table for this chunk
        await conn.execute(
            text("CREATE TEMP TABLE temp_mcd_consumer_activities (LIKE mcd_consumer_activities EXCLUDING ALL) ON COMMIT DROP")
        )
        
        # Stream load to staging table
        await asyncpg_conn.copy_records_to_table(
            'temp_mcd_consumer_activities', records=batch, columns=columns
        )
        
        # Merge/Upsert query
        upsert_query = """
        INSERT INTO mcd_consumer_activities (
            activity_id,
            action_type_code,
            action_type_name,
            action_type_detail,
            device_type_code,
            device_type_name,
            market,
            reporting_id,
            date,
            activity_source_time_local,
            activity_source_time_utc,
            source_activity_offset,
            hour,
            time_of_processed,
            market_id,
            download_count,
            startup_count,
            registration_count,
            login_count,
            email_registration_count,
            device_registration_count,
            consumer_token_generation_count,
            mfa_token_generation_count,
            defaultapp_email_registration_count,
            altapp1_email_registration_count
        )
        SELECT 
            activity_id,
            action_type_code,
            action_type_name,
            action_type_detail,
            device_type_code,
            device_type_name,
            market,
            reporting_id,
            date,
            activity_source_time_local,
            activity_source_time_utc,
            source_activity_offset,
            hour,
            time_of_processed,
            market_id,
            download_count,
            startup_count,
            registration_count,
            login_count,
            email_registration_count,
            device_registration_count,
            consumer_token_generation_count,
            mfa_token_generation_count,
            defaultapp_email_registration_count,
            altapp1_email_registration_count
        FROM temp_mcd_consumer_activities
        ON CONFLICT (activity_id) DO UPDATE SET
            action_type_code = EXCLUDED.action_type_code,
            action_type_name = EXCLUDED.action_type_name,
            action_type_detail = EXCLUDED.action_type_detail,
            device_type_code = EXCLUDED.device_type_code,
            device_type_name = EXCLUDED.device_type_name,
            market = EXCLUDED.market,
            reporting_id = EXCLUDED.reporting_id,
            date = EXCLUDED.date,
            activity_source_time_local = EXCLUDED.activity_source_time_local,
            activity_source_time_utc = EXCLUDED.activity_source_time_utc,
            source_activity_offset = EXCLUDED.source_activity_offset,
            hour = EXCLUDED.hour,
            time_of_processed = EXCLUDED.time_of_processed,
            market_id = EXCLUDED.market_id,
            download_count = EXCLUDED.download_count,
            startup_count = EXCLUDED.startup_count,
            registration_count = EXCLUDED.registration_count,
            login_count = EXCLUDED.login_count,
            email_registration_count = EXCLUDED.email_registration_count,
            device_registration_count = EXCLUDED.device_registration_count,
            consumer_token_generation_count = EXCLUDED.consumer_token_generation_count,
            mfa_token_generation_count = EXCLUDED.mfa_token_generation_count,
            defaultapp_email_registration_count = EXCLUDED.defaultapp_email_registration_count,
            altapp1_email_registration_count = EXCLUDED.altapp1_email_registration_count;
        """
        await conn.execute(text(upsert_query))

@task(retries=3, retry_delay_seconds=10, log_prints=True)
async def extract_and_load_campaign_fast():
    client = AsyncIOMotorClient(
        mongo_uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000, 
    )
    db = client[mongo_db]
    
    # Stream from MongoDB using cursor
    cursor = db["mcd_consumer_activities"].find().batch_size(100000)
    
    columns = [
        "activity_id",
        "action_type_code",
        "action_type_name",
        "action_type_detail",
        "device_type_code",
        "device_type_name",
        "market",
        "reporting_id",
        "date",
        "activity_source_time_local",
        "activity_source_time_utc",
        "source_activity_offset",
        "hour",
        "time_of_processed",
        "market_id",
        "download_count",
        "startup_count",
        "registration_count",
        "login_count",
        "email_registration_count",
        "device_registration_count",
        "consumer_token_generation_count",
        "mfa_token_generation_count",
        "defaultapp_email_registration_count",
        "altapp1_email_registration_count
    ]
    
    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0
    
    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")
    
    async for doc in cursor:
        row = (
            to_uuid(doc.get("activity_id")),
            (doc.get("action_type_code")),
            (doc.get("action_type_name")),
            (doc.get("action_type_detail")),
            (doc.get("device_type_code")),
            (doc.get("device_type_name")),
            (doc.get("market")),
            (doc.get("reporting_id")),
            (doc.get("date")),
            (doc.get("activity_source_time_local")),
            (doc.get("activity_source_time_utc")),
            (doc.get("source_activity_offset")),
            (doc.get("hour")),
            (doc.get("time_of_processed")),
            (doc.get("market_id")),
            (doc.get("download_count")),
            (doc.get("startup_count")),
            (doc.get("registration_count")),
            (doc.get("login_count")),
            (doc.get("email_registration_count")),
            (doc.get("device_registration_count")),
            (doc.get("consumer_token_generation_count")),
            (doc.get("mfa_token_generation_count")),
            (doc.get("defaultapp_email_registration_count")),
            (doc.get("altapp1_email_registration_count"))
        )

        batch.append(row)
        
        if len(batch) >= chunk_size:
            await load_chunk_to_postgres(batch, columns)
            total += len(batch)
            print(f"   ✅ Synchronized chunk of {len(batch)} consumer activities (Total: {total})")
            batch = []
            # Beri jeda ke event loop agar Prefect bisa mengirim heartbeat
            await asyncio.sleep(0.1)
            
    if batch:
        await load_chunk_to_postgres(batch, columns)
        total += len(batch)
        print(f"   ✅ Synchronized chunk of {len(batch)} consumer activities (Total: {total})")
        
    print(f"🚀 Bulk Sync Finished! Total {total} consumer activities successfully upserted.")

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_consumer_activities_flow():
    print("ETL Flow consumer activities started...")
    await extract_and_load_campaign_fast()
    print("ETL Flow consumer activities finished!")

if __name__ == "__main__":
    asyncio.run(etl_consumer_activities_flow())