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
            text("CREATE TEMP TABLE temp_mcd_loyalty_points_card_transactions (LIKE mcd_loyalty_points_card_transactions EXCLUDING ALL) ON COMMIT DROP")
        )
        
        # Stream load to staging table
        await asyncpg_conn.copy_records_to_table(
            'temp_mcd_loyalty_points_card_transactions', records=batch, columns=columns
        )
        
        # Merge/Upsert query
        upsert_query = """
        INSERT INTO mcd_loyalty_points_card_transactions (
            points_card_transaction_id,
            points_card_transaction_group_id,  
            pos_sales_transaction_id,
            transaction_time_utc, 
            transaction_source_time_local,     
            transaction_source_time_utc_offset,
            transaction_type,
            transaction_sub_type,      
            transaction_origin,     
            note,       
            points_balance_after_transaction,  
            reward_type,
            reporting_id,              
            incentive_program_id,      
            venue_id,     
            referenced_transactions,   
            points_delta,  
            external_transaction_reference,    
            sale_id,
            venue_external_id,         
            bonus_points_total                     
        )
        SELECT 
            points_card_transaction_id,
            points_card_transaction_group_id,  
            pos_sales_transaction_id,
            transaction_time_utc, 
            transaction_source_time_local,     
            transaction_source_time_utc_offset,
            transaction_type,
            transaction_sub_type,      
            transaction_origin,     
            note,       
            points_balance_after_transaction,  
            reward_type,
            reporting_id,              
            incentive_program_id,      
            venue_id,     
            referenced_transactions,   
            points_delta,  
            external_transaction_reference,    
            sale_id,
            venue_external_id,         
            bonus_points_total               
        FROM mcd_loyalty_points_card_transactions
        ON CONFLICT (points_card_transaction_id) DO UPDATE SET
            points_card_transaction_group_id = EXCLUDED.points_card_transaction_group_id,  
            pos_sales_transaction_id = EXCLUDED.pos_sales_transaction_id,
            transaction_time_utc = EXCLUDED.transaction_time_utc, 
            transaction_source_time_local = EXCLUDED.transaction_source_time_local,
            transaction_source_time_utc_offset = EXCLUDED.transaction_source_time_utc_offset,
            transaction_type = EXCLUDED.transaction_type,
            transaction_sub_type = EXCLUDED.transaction_sub_type,
            transaction_origin = EXCLUDED.transaction_origin,
            note = EXCLUDED.note,
            points_balance_after_transaction = EXCLUDED.points_balance_after_transaction,  
            reward_type = EXCLUDED.reward_type,
            reporting_id = EXCLUDED.reporting_id,
            incentive_program_id = EXCLUDED.incentive_program_id,
            venue_id = EXCLUDED.venue_id,
            referenced_transactions = EXCLUDED.referenced_transactions,
            points_delta = EXCLUDED.points_delta,
            external_transaction_reference = EXCLUDED.external_transaction_reference, 
            sale_id = EXCLUDED.sale_id,
            venue_external_id = EXCLUDED.venue_external_id,
            bonus_points_total = EXCLUDED.bonus_points_total;
        """
        await conn.execute(text(upsert_query))

@task(retries=3, retry_delay_seconds=10, log_prints=True)
async def extract_and_load_loyalty_points_card_transactions_fast():
    client = AsyncIOMotorClient(
        mongo_uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000, 
    )
    db = client[mongo_db]
    
    # Stream from MongoDB using cursor
    cursor = db["mcd_loyalty_points_card_transactions"].find().batch_size(100000)
    
    columns = [
        "points_card_transaction_id",
        "points_card_transaction_group_id",  
        "pos_sales_transaction_id",
        "transaction_time_utc", 
        "transaction_source_time_local",     
        "transaction_source_time_utc_offset",
        "transaction_type",
        "transaction_sub_type",      
        "transaction_origin",     
        "note",       
        "points_balance_after_transaction",  
        "reward_type",
        "reporting_id",              
        "incentive_program_id",      
        "venue_id",     
        "referenced_transactions",   
        "points_delta",  
        "external_transaction_reference",    
        "sale_id",
        "venue_external_id",         
        "bonus_points_total"  
    ]
    
    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0
    
    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")
    
    async for doc in cursor:
        row = (
            doc.get("points_card_transaction_id"),
            doc.get("points_card_transaction_group_id"),  
            doc.get("pos_sales_transaction_id"),
            doc.get("transaction_time_utc"), 
            doc.get("transaction_source_time_local"),     
            doc.get("transaction_source_time_utc_offset"),
            doc.get("transaction_type"),
            doc.get("transaction_sub_type"),      
            doc.get("transaction_origin"),     
            doc.get("note"),       
            doc.get("points_balance_after_transaction"),  
            doc.get("reward_type"),
            doc.get("reporting_id"),              
            doc.get("incentive_program_id"),      
            doc.get("venue_id"),     
            doc.get("referenced_transactions"),   
            doc.get("points_delta"),  
            doc.get("external_transaction_reference"),    
            doc.get("sale_id"),
            doc.get("venue_external_id"),         
            doc.get("bonus_points_total")
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
async def etl_loyalty_points_card_transactions_flow():
    print("ETL Flow advertisement started...")
    await extract_and_load_loyalty_points_card_transactions_fast()
    print("ETL Flow advertisement finished!")

if __name__ == "__main__":
    asyncio.run(etl_loyalty_points_card_transactions_flow())