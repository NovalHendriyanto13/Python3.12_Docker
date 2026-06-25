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
            sale_id,
            pos_transaction_id,
            reporting_id,
            offer_ids,
            venue_external_id,
            total_amount,
            tax_total_amount,
            gross_amount,
            before_discount_tax_total_amount,
            before_discount_total_amount,
            day_part,
            pod_type,
            transaction_kind,
            order_take_platform,
            sale_type,
            date,
            market,
            transaction_source_time_local,
            plexure_processing_time_utc,
            internal_id        
        )
        SELECT 
            sale_id,
            pos_transaction_id,
            reporting_id,
            offer_ids,
            venue_external_id,
            total_amount,
            tax_total_amount,
            gross_amount,
            before_discount_tax_total_amount,
            before_discount_total_amount,
            day_part,
            pod_type,
            transaction_kind,
            order_take_platform,
            sale_type,
            date,
            market,
            transaction_source_time_local,
            plexure_processing_time_utc,
            internal_id     
        FROM temp_mcd_sales_headers
        ON CONFLICT (sale_id) DO UPDATE SET
            pos_transaction_id = EXCLUDED.pos_transaction_id,
            reporting_id = EXCLUDED.reporting_id,
            offer_ids = EXCLUDED.offer_ids,
            venue_external_id = EXCLUDED.venue_external_id,
            total_amount = EXCLUDED.total_amount,
            tax_total_amount = EXCLUDED.tax_total_amount,
            gross_amount = EXCLUDED.gross_amount,
            before_discount_tax_total_amount = EXCLUDED.before_discount_tax_total_amount,
            before_discount_total_amount = EXCLUDED.before_discount_total_amount,
            day_part = EXCLUDED.day_part,
            pod_type = EXCLUDED.pod_type,
            transaction_kind = EXCLUDED.transaction_kind,
            order_take_platform = EXCLUDED.order_take_platform,
            sale_type = EXCLUDED.sale_type,
            date = EXCLUDED.date,
            market = EXCLUDED.market,
            transaction_source_time_local = EXCLUDED.transaction_source_time_local,
            plexure_processing_time_utc = EXCLUDED.plexure_processing_time_utc,
            internal_id = EXCLUDED.internal_id;
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
        "sale_id",
        "pos_transaction_id",
        "reporting_id",
        "offer_ids",
        "venue_external_id",
        "total_amount",
        "tax_total_amount",
        "gross_amount",
        "before_discount_tax_total_amount",
        "before_discount_total_amount",
        "day_part",
        "pod_type",
        "transaction_kind",
        "order_take_platform",
        "sale_type",
        "date",
        "market",
        "transaction_source_time_local",
        "plexure_processing_time_utc",
        "internal_id"
    ]
    
    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0
    
    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")
    
    async for doc in cursor:
        row = (
            (doc.get("saleid")),
            to_uuid(doc.get("postransactionid")),
            to_uuid(doc.get("reportingid")),
            (doc.get("offerids")),
            (doc.get("venue_external_id")),
            to_decimal(doc.get("total_amount")),
            to_decimal(doc.get("tax_total_amount")),
            to_decimal(doc.get("gross_amount")),
            to_decimal(doc.get("before_discount_tax_total_amount")),
            to_decimal(doc.get("before_discount_total_amount")),
            (doc.get("day_part")),
            (doc.get("pod_type")),
            (doc.get("transaction_kind")),
            (doc.get("order_take_platform")),
            (doc.get("sale_type")),
            to_datetime(doc.get("date")),
            (doc.get("market")),
            to_datetime(doc.get("transaction_source_time_local")),
            to_datetime(doc.get("plexure_processing_time_utc")),
            doc.get("internal_id")
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