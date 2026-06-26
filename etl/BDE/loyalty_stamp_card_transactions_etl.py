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
            text("CREATE TEMP TABLE temp_mcd_loyalty_stamp_card_transactions (LIKE mcd_loyalty_stamp_card_transactions EXCLUDING ALL) ON COMMIT DROP")
        )
        
        # Stream load to staging table
        await asyncpg_conn.copy_records_to_table(
            'temp_mcd_loyalty_stamp_card_transactions', records=batch, columns=columns
        )
        
        # Merge/Upsert query
        upsert_query = """
        INSERT INTO mcd_loyalty_stamp_card_transactions (
            stamp_card_transaction_id,        
            pos_sales_transaction_id, 
            transaction_time_utc,
            transaction_source_time_local,
            transaction_source_time_utc_offset, 
            transaction_origin, 
            is_initial_rewarded_stamps, 
            note,
            stamp_card_id, 
            total_card_stamps_after_transaction, 
            reporting_id, 
            incentive_program_id,
            venue_id, 
            number_of_stamps_added, 
            external_transaction_reference, 
            sale_id,
            venue_external_id                      
        )
        SELECT 
            stamp_card_transaction_id,        
            pos_sales_transaction_id, 
            transaction_time_utc,
            transaction_source_time_local,
            transaction_source_time_utc_offset, 
            transaction_origin, 
            is_initial_rewarded_stamps, 
            note,
            stamp_card_id, 
            total_card_stamps_after_transaction, 
            reporting_id, 
            incentive_program_id,
            venue_id, 
            number_of_stamps_added, 
            external_transaction_reference, 
            sale_id,
            venue_external_id           
        FROM temp_mcd_loyalty_stamp_card_transactions
        ON CONFLICT (stamp_card_transaction_id) DO UPDATE SET   
            pos_sales_transaction_id = EXCLUDED.pos_sales_transaction_id,
            transaction_time_utc = EXCLUDED.transaction_time_utc,
            transaction_source_time_local = EXCLUDED.transaction_source_time_local,
            transaction_source_time_utc_offset = EXCLUDED.transaction_source_time_utc_offset,
            transaction_origin = EXCLUDED.transaction_origin,
            is_initial_rewarded_stamps = EXCLUDED.is_initial_rewarded_stamps,
            note = EXCLUDED.note,
            stamp_card_id = EXCLUDED.stamp_card_id,
            total_card_stamps_after_transaction = EXCLUDED.total_card_stamps_after_transaction,
            reporting_id = EXCLUDED.reporting_id,
            incentive_program_id = EXCLUDED.incentive_program_id,
            venue_id = EXCLUDED.venue_id,
            number_of_stamps_added = EXCLUDED.number_of_stamps_added,
            external_transaction_reference = EXCLUDED.external_transaction_reference,
            sale_id = EXCLUDED.sale_id,
            venue_external_id = EXCLUDED.venue_external_id;
        """
        await conn.execute(text(upsert_query))

@task(retries=3, retry_delay_seconds=10, log_prints=True)
async def extract_and_load_loyalty_stamp_card_transactions_fast():
    client = AsyncIOMotorClient(
        mongo_uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000, 
    )
    db = client[mongo_db]
    
    # Stream from MongoDB using cursor
    cursor = db["mcd_loyalty_stamp_card_transactions"].find().batch_size(100000)
    
    columns = [
        "stamp_card_transaction_id",     
        "pos_sales_transaction_id", 
        "transaction_time_utc",
        "transaction_source_time_local",
        "transaction_source_time_utc_offset", 
        "transaction_origin", 
        "is_initial_rewarded_stamps", 
        "note",
        "stamp_card_id", 
        "total_card_stamps_after_transaction", 
        "reporting_id", 
        "incentive_program_id",
        "venue_id", 
        "number_of_stamps_added", 
        "external_transaction_reference", 
        "sale_id",
        "venue_external_id"
    ]
    
    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0
    
    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")
    
    async for doc in cursor:
        row = (
             to_uuid(doc.get("stamp_card_transaction_id")),     
            to_uuid(doc.get("pos_sales_transaction_id")), 
            to_datetime(doc.get("transaction_time_utc")),
            to_datetime(doc.get("transaction_source_time_local")),
            doc.get("transaction_source_time_utc_offset"), 
            doc.get("transaction_origin"), 
            to_bool(doc.get("is_initial_rewarded_stamps")), 
            doc.get("note"),
            to_uuid(doc.get("stamp_card_id")), 
            to_int(doc.get("total_card_stamps_after_transaction")), 
            to_uuid(doc.get("reporting_id")), 
            to_int(doc.get("incentive_program_id")),
            doc.get("venue_id"), 
            to_int(doc.get("number_of_stamps_added")), 
            doc.get("external_transaction_reference"), 
            doc.get("sale_id"),
            doc.get("venue_external_id")
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
        print(f"   ✅ Synchronized chunk of {len(batch)} loyalty points card transactions (Total: {total})")
        
    print(f"🚀 Bulk Sync Finished! Total {total} loyalty points card transactions successfully upserted.")

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_loyalty_stamp_card_transactions_flow():
    print("ETL Flow loyalty point card transaction started...")
    await extract_and_load_loyalty_stamp_card_transactions_fast()
    print("ETL Flow loyalty point card transaction finished!")

if __name__ == "__main__":
    asyncio.run(etl_loyalty_stamp_card_transactions_flow())