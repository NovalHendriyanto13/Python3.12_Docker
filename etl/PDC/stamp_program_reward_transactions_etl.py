import sys
import os
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_bool

# Helper function to load a single chunk to PostgreSQL using COPY + Merge
async def load_chunk_to_postgres(batch: list, columns: list):
    async with engine.begin() as conn:
        raw_conn = await conn.get_raw_connection()
        asyncpg_conn = raw_conn.driver_connection
        
        # Create staging table for this chunk
        await conn.execute(
            text("CREATE TEMP TABLE temp_mcd_stamp_program_reward_transactions (LIKE mcd_stamp_program_reward_transactions EXCLUDING ALL) ON COMMIT DROP")
        )
        
        # Stream load to staging table
        await asyncpg_conn.copy_records_to_table(
            'temp_mcd_stamp_program_reward_transactions', records=batch, columns=columns
        )
        
        # Merge/Upsert query
        upsert_query = """
        INSERT INTO mcd_stamp_program_reward_transactions (
            stamp_program_reward_transaction_id,
            stamp_program_reward_transaction_group_id,
            pos_sales_transaction_id,
            sale_id,
            incentive_program_id,
            incentive_program_name,
            stamp_reward_transaction_type,
            stamp_card_id,
            card_capacity,
            reward_type,
            reward_reference_id,
            venue_id,
            venue_name,
            venue_external_id,
            market,
            reporting_id,
            date,
            transaction_source_time_local,
            transaction_time_utc
        )
        SELECT 
            stamp_program_reward_transaction_id,
            stamp_program_reward_transaction_group_id,
            pos_sales_transaction_id,
            sale_id,
            incentive_program_id,
            incentive_program_name,
            stamp_reward_transaction_type,
            stamp_card_id,
            card_capacity,
            reward_type,
            reward_reference_id,
            venue_id,
            venue_name,
            venue_external_id,
            market,
            reporting_id,
            date,
            transaction_source_time_local,
            transaction_time_utc     
        FROM temp_mcd_stamp_program_reward_transactions
        ON CONFLICT (stamp_program_reward_transaction_id) DO UPDATE SET
            stamp_program_reward_transaction_group_id = EXCLUDED.stamp_program_reward_transaction_group_id,
            pos_sales_transaction_id = EXCLUDED.pos_sales_transaction_id,
            sale_id = EXCLUDED.sale_id,
            incentive_program_id = EXCLUDED.incentive_program_id,
            incentive_program_name = EXCLUDED.incentive_program_name,
            stamp_reward_transaction_type = EXCLUDED.stamp_reward_transaction_type,
            stamp_card_id = EXCLUDED.stamp_card_id,
            card_capacity = EXCLUDED.card_capacity,
            reward_type = EXCLUDED.reward_type,
            reward_reference_id = EXCLUDED.reward_reference_id,
            venue_id = EXCLUDED.venue_id,
            venue_name = EXCLUDED.venue_name,
            venue_external_id = EXCLUDED.venue_external_id,
            market = EXCLUDED.market,
            reporting_id = EXCLUDED.reporting_id,
            date = EXCLUDED.date,
            transaction_source_time_local = EXCLUDED.transaction_source_time_local,
            transaction_time_utc = EXCLUDED.transaction_time_utc;
        """
        await conn.execute(text(upsert_query))

@task(retries=3, retry_delay_seconds=10, log_prints=True)
async def extract_and_load_stamp_program_reward_transactions_fast():
    client = AsyncIOMotorClient(
        mongo_uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000, 
    )
    db = client[mongo_db]
    
    # Stream from MongoDB using cursor
    cursor = db["mcd_stamp_program_reward_transactions"].find().batch_size(100000)
    
    columns = [
        "stamp_program_reward_transaction_id",
        "stamp_program_reward_transaction_group_id",
        "pos_sales_transaction_id",
        "sale_id",
        "incentive_program_id",
        "incentive_program_name",
        "stamp_reward_transaction_type",
        "stamp_card_id",
        "card_capacity",
        "reward_type",
        "reward_reference_id",
        "venue_id",
        "venue_name",
        "venue_external_id",
        "market",
        "reporting_id",
        "date",
        "transaction_source_time_local",
        "transaction_time_utc"
    ]
    
    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0
    
    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")
    
    async for doc in cursor:
        row = (
            to_uuid(doc.get("stamp_program_reward_transaction_id")),
            to_uuid(doc.get("stamp_program_reward_transaction_group_id")),
            to_uuid(doc.get("pos_sales_transaction_id")),
            to_uuid(doc.get("sale_id")),
            to_int(doc.get("incentive_program_id")),
            (doc.get("incentive_program_name")),
            (doc.get("stamp_reward_transaction_type")),
            (doc.get("stamp_card_id")),
            to_int(doc.get("card_capacity")),
            (doc.get("reward_type")),
            (doc.get("reward_reference_id")),
            to_int(doc.get("venue_id")),
            (doc.get("venue_name")),
            (doc.get("venue_external_id")),
            (doc.get("market")),
            to_uuid(doc.get("reporting_id")),
            to_datetime(doc.get("date")),
            to_datetime(doc.get("transaction_source_time_local")),
            to_datetime(doc.get("transaction_time_utc"))
        )
        batch.append(row)
        
        if len(batch) >= chunk_size:
            await load_chunk_to_postgres(batch, columns)
            total += len(batch)
            print(f"   ✅ Synchronized chunk of {len(batch)} loyalty points card transactions (Total: {total})")
            batch = []
            # Beri jeda ke event loop agar Prefect bisa mengirim heartbeat
            await asyncio.sleep(0.1)
            
    if batch:
        await load_chunk_to_postgres(batch, columns)
        total += len(batch)
        print(f"   ✅ Synchronized chunk of {len(batch)} loyalty points card reward transactions (Total: {total})")
        
    print(f"🚀 Bulk Sync Finished! Total {total} loyalty points card  reward transactions successfully upserted.")

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_stamp_program_reward_transactions_flow():
    print("ETL Flow loyalty point card reward transaction started...")
    await extract_and_load_stamp_program_reward_transactions_fast()
    print("ETL Flow loyalty point card reward transaction finished!")

if __name__ == "__main__":
    asyncio.run(etl_stamp_program_reward_transactions_flow())