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

# Helper function to load a single chunk to PostgreSQL using COPY + Merge
async def load_chunk_to_postgres(batch: list, columns: list):
    async with engine.begin() as conn:
        raw_conn = await conn.get_raw_connection()
        asyncpg_conn = raw_conn.driver_connection
        
        # Create staging table for this chunk
        await conn.execute(
            text("CREATE TEMP TABLE temp_mcd_consumer_external_ids (LIKE mcd_consumer_external_ids EXCLUDING ALL) ON COMMIT DROP")
        )
        
        # Stream load to staging table
        await asyncpg_conn.copy_records_to_table(
            'temp_mcd_consumer_external_ids', records=batch, columns=columns
        )
        
        # Merge/Upsert query
        upsert_query = """
        INSERT INTO mcd_consumer_external_ids (
            reporting_id,
            external_id,
            market,
            deleted_flag,
            timestamp
        )
        SELECT 
            reporting_id,
            external_id,
            market,
            deleted_flag,
            timestamp
        FROM temp_mcd_consumer_external_ids
        ON CONFLICT (reporting_id, external_id) DO UPDATE SET
            reporting_id = EXCLUDED.reporting_id,
            external_id = EXCLUDED.external_id,
            market = EXCLUDED.market,
            deleted_flag = EXCLUDED.deleted_flag,
            timestamp = EXCLUDED.timestamp;
        """
        await conn.execute(text(upsert_query))

@task(retries=3, retry_delay_seconds=10, log_prints=True)
async def extract_and_load_consumer_external_id_fast():
    client = AsyncIOMotorClient(
        mongo_uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000, 
    )
    db = client[mongo_db]
    
    # Stream from MongoDB using cursor
    cursor = db["mcd_consumer_external_ids"].find().batch_size(100000)
    
    columns = [
        "reporting_id",
        "external_id",
        "market",
        "deleted_flag",
        "timestamp"
    ]
    
    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0
    
    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")
    
    async for doc in cursor:
        reporting_id = to_uuid(doc.get("reporting_id"))

        if reporting_id is None:
            continue

        row = (
            reporting_id,
            doc.get("external_id"),
            doc.get("market"),
            doc.get("deleted_flag"),
            to_datetime(doc.get("timestamp")),
        )

        batch.append(row)
        
        if len(batch) >= chunk_size:
            await load_chunk_to_postgres(batch, columns)
            total += len(batch)
            print(f"   ✅ Synchronized chunk of {len(batch)} campaigns (Total: {total})")
            batch = []
            # Beri jeda ke event loop agar Prefect bisa mengirim heartbeat
            await asyncio.sleep(0.1)
            
    if batch:
        await load_chunk_to_postgres(batch, columns)
        total += len(batch)
        print(f"   ✅ Synchronized chunk of {len(batch)} customer external id (Total: {total})")
        
    print(f"🚀 Bulk Sync Finished! Total {total} customer external id successfully upserted.")

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_consumer_externalID_flow():
    print("ETL Flow customer external id started...")
    await extract_and_load_consumer_external_id_fast()
    print("ETL Flow customer external id finished!")

if __name__ == "__main__":
    asyncio.run(etl_consumer_externalID_flow())