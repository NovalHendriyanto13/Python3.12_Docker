import sys
import os
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db

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
            id, campaignid, title, description, startdate, enddate, status     
        )
        SELECT 
            id, campaignid, title, description, startdate, enddate, status 
        FROM mcd_advertisements
        ON CONFLICT (id) DO UPDATE SET
            campaignid = EXCLUDED.campaignid,
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            startdate = EXCLUDED.startdate,
            enddate = EXCLUDED.enddate,
            status = EXCLUDED.status;
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
        "id", "campaignid", "title", "description", "startdate", "enddate", "status"  
    ]
    
    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0
    
    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")
    
    async for doc in cursor:
        row = (
            doc.get("id"),
            doc.get("campaignid"),
            doc.get("title"),
            doc.get("description"),
            doc.get("startdate"),
            doc.get("enddate"),
            doc.get("status")
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
async def etl_advertisement_flow():
    print("ETL Flow advertisement started...")
    await extract_and_load_advertisements_fast()
    print("ETL Flow advertisement finished!")

if __name__ == "__main__":
    asyncio.run(etl_advertisement_flow())