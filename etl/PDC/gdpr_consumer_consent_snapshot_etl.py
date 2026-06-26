import sys
import os
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_bool, to_decimal

# Helper function to load a single chunk to PostgreSQL using COPY + Merge
async def load_chunk_to_postgres(batch: list, columns: list):
    async with engine.begin() as conn:
        raw_conn = await conn.get_raw_connection()
        asyncpg_conn = raw_conn.driver_connection
        
        # Create staging table for this chunk
        await conn.execute(
            text("CREATE TEMP TABLE temp_mcd_gdpr_consumer_consent_snapshot (LIKE mcd_gdpr_consumer_consent_snapshot EXCLUDING ALL) ON COMMIT DROP")
        )
        
        # Stream load to staging table
        await asyncpg_conn.copy_records_to_table(
            'temp_mcd_gdpr_consumer_consent_snapshot', records=batch, columns=columns
        )
        
        # Merge/Upsert query
        upsert_query = """
        INSERT INTO mcd_gdpr_consumer_consent_snapshot (
            reporting_id,
            market,
            consent_to_store_and_process,
            services,
            event_time_utc,
            date
        )
        SELECT 
            reporting_id,
            market,
            consent_to_store_and_process,
            services,
            event_time_utc,
            date    
        FROM temp_mcd_gdpr_consumer_consent_snapshot
        ON CONFLICT (reporting_id, market, consent_to_store_and_process, services, event_time_utc, date) DO UPDATE SET
            reporting_id = EXCLUDED.reporting_id,
            market = EXCLUDED.market,
            consent_to_store_and_process = EXCLUDED.consent_to_store_and_process,
            services = EXCLUDED.services,
            event_time_utc = EXCLUDED.event_time_utc,
            date = EXCLUDED.date;
        """
        await conn.execute(text(upsert_query))

@task(retries=3, retry_delay_seconds=10, log_prints=True)
async def extract_and_load_gdpr_consumer_consent_snapshot_fast():
    client = AsyncIOMotorClient(
        mongo_uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000, 
    )
    db = client[mongo_db]
    
    # Stream from MongoDB using cursor
    cursor = db["mcd_gdpr_consumer_consent_snapshot"].find().batch_size(100000)
    
    columns = [
        "reporting_id",
        "market",
        "consent_to_store_and_process",
        "services",
        "event_time_utc",
        "date"
    ]
    
    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0
    
    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")
    
    async for doc in cursor:
        row = (
            to_uuid(doc.get("reporting_id")),
            (doc.get("market")),
            to_bool(doc.get("consent_to_store_and_process")),
            (doc.get("services")),
            to_datetime(doc.get("event_time_utc")),
            to_datetime(doc.get("date"))
        )
        batch.append(row)
        
        if len(batch) >= chunk_size:
            await load_chunk_to_postgres(batch, columns)
            total += len(batch)
            print(f"   ✅ Synchronized chunk of {len(batch)} gdpr_consumer_consent_snapshot (Total: {total})")
            batch = []
            # Beri jeda ke event loop agar Prefect bisa mengirim heartbeat
            await asyncio.sleep(0.1)
            
    if batch:
        await load_chunk_to_postgres(batch, columns)
        total += len(batch)
        print(f"   ✅ Synchronized chunk of {len(batch)} gdpr_consumer_consent_snapshot (Total: {total})")
        
    print(f"🚀 Bulk Sync Finished! Total {total} gdpr_consumer_consent_snapshot successfully upserted.")

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_gdpr_consumer_consent_snapshot_flow():
    print("ETL Flow gdpr_consumer_consent_snapshot started...")
    await extract_and_load_gdpr_consumer_consent_snapshot_fast()
    print("ETL Flow gdpr_consumer_consent_snapshot finished!")

if __name__ == "__main__":
    asyncio.run(etl_gdpr_consumer_consent_snapshot_flow())