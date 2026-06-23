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
            text("CREATE TEMP TABLE temp_mcd_sales_headers (LIKE mcd_sales_headers EXCLUDING ALL) ON COMMIT DROP")
        )
        
        # Stream load to staging table
        await asyncpg_conn.copy_records_to_table(
            'temp_mcd_sales_headers', records=batch, columns=columns
        )
        
        # Merge/Upsert query
        upsert_query = """
        INSERT INTO mcd_sales_headers (
            reporting_id,
            sale_id,
            total_amount,
            offer_id,
            internal_id,
            gross_amount,
            tax_total_amount,
            before_discount_tax_total_amount,
            before_discount_total_amount,
            day_part,
            pod_type,
            transaction_kind,
            order_take_platform,
            sale_type,
            venue_id,
            date_occurred,
            pos_transaction_id                      
        )
        SELECT 
            reporting_id,
            sale_id,
            total_amount,
            offer_id,
            internal_id,
            gross_amount,
            tax_total_amount,
            before_discount_tax_total_amount,
            before_discount_total_amount,
            day_part,
            pod_type,
            transaction_kind,
            order_take_platform,
            sale_type,
            venue_id,
            date_occurred,
            pos_transaction_id          
        FROM temp_mcd_sales_headers
        ON CONFLICT (stamp_card_reward_transaction_id) DO UPDATE SET
            reporting_id,
            sale_id,
            total_amount,
            offer_id,
            internal_id,
            gross_amount,
            tax_total_amount,
            before_discount_tax_total_amount,
            before_discount_total_amount,
            day_part,
            pod_type,
            transaction_kind,
            order_take_platform,
            sale_type,
            venue_id,
            date_occurred,
            pos_transaction_id;
        """
        await conn.execute(text(upsert_query))

@task(retries=3, retry_delay_seconds=10, log_prints=True)
async def extract_and_load_sales_headers_fast():
    client = AsyncIOMotorClient(
        mongo_uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000, 
    )
    db = client[mongo_db]
    
    # Stream from MongoDB using cursor
    cursor = db["mcd_sales_headers"].find().batch_size(100000)
    
    columns = [
        "reporting_id",
        "sale_id",
        "total_amount",
        "offer_id",
        "internal_id",
        "gross_amount",
        "tax_total_amount",
        "before_discount_tax_total_amount",
        "before_discount_total_amount",
        "day_part",
        "pod_type",
        "transaction_kind",
        "order_take_platform",
        "sale_type",
        "venue_id",
        "date_occurred",
        "pos_transaction_id"
    ]
    
    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0
    
    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")
    
    async for doc in cursor:
        row = (
            to_uuid(doc.get("reporting_id")),
            doc.get("sale_id"),
            to_decimal(doc.get("total_amount")),
            to_int(doc.get("offer_id")),
            doc.get("internal_id"),
            to_decimal(doc.get("gross_amount")),
            to_decimal(doc.get("tax_total_amount")),
            to_decimal(doc.get("before_discount_tax_total_amount")),
            to_decimal(doc.get("before_discount_total_amount")),
            doc.get("day_part"),
            doc.get("pod_type"),
            doc.get("transaction_kind"),
            doc.get("order_take_platform"),
            doc.get("sale_type"),
            to_int(doc.get("venue_id")),
            to_datetime(doc.get("date_occurred")),
            to_uuid(doc.get("pos_transaction_id"))
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
        print(f"   ✅ Synchronized chunk of {len(batch)} sales header (Total: {total})")
        
    print(f"🚀 Bulk Sync Finished! Total {total} sales header successfully upserted.")

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_sales_headers_flow():
    print("ETL Flow sales header started...")
    await extract_and_load_sales_headers_fast()
    print("ETL Flow sales header finished!")

if __name__ == "__main__":
    asyncio.run(etl_sales_headers_flow())