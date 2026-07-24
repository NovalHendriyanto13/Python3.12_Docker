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
            text("CREATE TEMP TABLE temp_mcd_campaigns (LIKE mcd_campaigns EXCLUDING ALL) ON COMMIT DROP")
        )
        
        # Stream load to staging table
        await asyncpg_conn.copy_records_to_table(
            'temp_mcd_campaigns', records=batch, columns=columns
        )
        
        # Merge/Upsert query
        upsert_query = """
        INSERT INTO mcd_campaigns (
            campaign_id,
            market,
            title,
            campaign_status,
            status,
            creation_date,
            modified_date
        )
        SELECT 
            campaign_id,
            market,
            title,
            campaign_status,
            status,
            creation_date,
            modified_date
        FROM temp_mcd_campaigns
        ON CONFLICT (campaign_id) DO UPDATE SET
            campaign_id = EXCLUDED.campaign_id,
            market = EXCLUDED.market,
            title = EXCLUDED.title,
            campaign_status = EXCLUDED.campaign_status,
            status = EXCLUDED.status,
            creation_date = EXCLUDED.creation_date,
            modified_date = EXCLUDED.modified_date;
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
    cursor = db["mcd_campaigns"].find().batch_size(100000)
    
    columns = [
        "campaign_id",
        "market",
        "title",
        "campaign_status",
        "status",
        "creation_date",
        "modified_date"
    ]
    
    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0
    
    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")
    
    async for doc in cursor:
        row = (
            to_int(doc.get("id")),
            doc.get("market"),
            doc.get("title"),
            campaign_status[doc.get("status")],
            doc.get("status"),
            to_datetime(doc.get("creation_date")),
            to_datetime(doc.get("modfied_date"))
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
        print(f"   ✅ Synchronized chunk of {len(batch)} campaigns (Total: {total})")
        
    print(f"🚀 Bulk Sync Finished! Total {total} campaigns successfully upserted.")

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_campaigns_flow():
    print("ETL Flow campaigns started...")
    await extract_and_load_campaign_fast()
    print("ETL Flow campaigns finished!")

if __name__ == "__main__":
    asyncio.run(etl_campaigns_flow())