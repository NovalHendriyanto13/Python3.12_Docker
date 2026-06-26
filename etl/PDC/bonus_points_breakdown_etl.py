import sys
import os
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool, to_uuid, to_decimal
from configs.constants import device_types
import uuid

# Helper function to load a single chunk to PostgreSQL using COPY + Merge
async def load_chunk_to_postgres(batch: list, columns: list):
    async with engine.begin() as conn:
        raw_conn = await conn.get_raw_connection()
        asyncpg_conn = raw_conn.driver_connection
        
        # Create staging table for this chunk
        await conn.execute(
            text("CREATE TEMP TABLE temp_mcd_bonus_points_breakdown (LIKE mcd_bonus_points_breakdown EXCLUDING ALL) ON COMMIT DROP")
        )
        
        # Stream load to staging table
        await asyncpg_conn.copy_records_to_table(
            'temp_mcd_bonus_points_breakdown', records=batch, columns=columns
        )
        
        # Merge/Upsert query
        upsert_query = """
        INSERT INTO mcd_bonus_points_breakdown (
            points_program_transaction_id,
            pos_sales_transaction_id,
            ordering_method,
            day_of_week,
            time_of_day,
            minimum_spend,
            product,
            satisfied_condition_type,
            bonus_rule_id,
            bonus_points,
            standard_points,
            venue_id,
            venue_name,
            venue_external_id,
            market,
            reporting_id,
            date,
            transaction_source_time_local,
            transaction_time_utc,
            bonus_points_breakdown_position
        )
        SELECT 
            points_program_transaction_id,
            pos_sales_transaction_id,
            ordering_method,
            day_of_week,
            time_of_day,
            minimum_spend,
            product,
            satisfied_condition_type,
            bonus_rule_id,
            bonus_points,
            standard_points,
            venue_id,
            venue_name,
            venue_external_id,
            market,
            reporting_id,
            date,
            transaction_source_time_local,
            transaction_time_utc,
            bonus_points_breakdown_position
        FROM temp_mcd_bonus_points_breakdown
        ON CONFLICT (points_program_transaction_id) DO UPDATE SET
            points_program_transaction_id = EXCLUDED.points_program_transaction_id,
            pos_sales_transaction_id = EXCLUDED.points_program_transaction_id,
            ordering_method = EXCLUDED.points_program_transaction_id,
            day_of_week = EXCLUDED.points_program_transaction_id,
            time_of_day = EXCLUDED.points_program_transaction_id,
            minimum_spend = EXCLUDED.points_program_transaction_id,
            product = EXCLUDED.points_program_transaction_id,
            satisfied_condition_type = EXCLUDED.points_program_transaction_id,
            bonus_rule_id = EXCLUDED.points_program_transaction_id,
            bonus_points = EXCLUDED.points_program_transaction_id,
            standard_points = EXCLUDED.points_program_transaction_id,
            venue_id = EXCLUDED.points_program_transaction_id,
            venue_name = EXCLUDED.points_program_transaction_id,
            venue_external_id = EXCLUDED.points_program_transaction_id,
            market = EXCLUDED.points_program_transaction_id,
            reporting_id = EXCLUDED.points_program_transaction_id,
            date = EXCLUDED.points_program_transaction_id,
            transaction_source_time_local = EXCLUDED.points_program_transaction_id,
            transaction_time_utc = EXCLUDED.points_program_transaction_id,
            bonus_points_breakdown_position = EXCLUDED.points_program_transaction_id;
        """
        await conn.execute(text(upsert_query))

@task(retries=3, retry_delay_seconds=10, log_prints=True)
async def extract_and_load_bonus_points_breakdown_fast():
    client = AsyncIOMotorClient(
        mongo_uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000, 
    )
    db = client[mongo_db]
    
    # Stream from MongoDB using cursor
    cursor = db["mcd_bonus_points_breakdown"].find().batch_size(100000)
    
    columns = [
        "points_program_transaction_id",
        "pos_sales_transaction_id",
        "ordering_method",
        "day_of_week",
        "time_of_day",
        "minimum_spend",
        "product",
        "satisfied_condition_type",
        "bonus_rule_id",
        "bonus_points",
        "standard_points",
        "venue_id",
        "venue_name",
        "venue_external_id",
        "market",
        "reporting_id",
        "date",
        "transaction_source_time_local",
        "transaction_time_utc",
        "bonus_points_breakdown_position"
    ]
    
    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0
    
    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")
    
    async for doc in cursor:
        row = (
            to_uuid(doc.get("points_program_transaction_id")),
            (doc.get("pos_sales_transaction_id")),
            (doc.get("ordering_method")),
            (doc.get("day_of_week")),
            (doc.get("time_of_day")),
            to_decimal(doc.get("minimum_spend")),
            (doc.get("product")),
            (doc.get("satisfied_condition_type")),
            (doc.get("bonus_rule_id")),
            to_int(doc.get("bonus_points")),
            to_int(doc.get("standard_points")),
            to_int(doc.get("venue_id")),
            (doc.get("venue_name")),
            (doc.get("venue_external_id")),
            (doc.get("market")),
            to_uuid(doc.get("reporting_id")),
            to_datetime(doc.get("date")),
            to_datetime(doc.get("transaction_source_time_local")),
            to_datetime(doc.get("transaction_time_utc")),
            to_int(doc.get("bonus_points_breakdown_position"))
        )

        batch.append(row)
        
        if len(batch) >= chunk_size:
            await load_chunk_to_postgres(batch, columns)
            total += len(batch)
            print(f"   ✅ Synchronized chunk of {len(batch)} bonus points breakdown (Total: {total})")
            batch = []
            # Beri jeda ke event loop agar Prefect bisa mengirim heartbeat
            await asyncio.sleep(0.1)
            
    if batch:
        await load_chunk_to_postgres(batch, columns)
        total += len(batch)
        print(f"   ✅ Synchronized chunk of {len(batch)} bonus points breakdown (Total: {total})")
        
    print(f"🚀 Bulk Sync Finished! Total {total} bonus points breakdown successfully upserted.")

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_bonus_points_breakdown_flow():
    print("ETL Flow bonus points breakdown started...")
    await extract_and_load_bonus_points_breakdown_fast()
    print("ETL Flow bonus points breakdown finished!")

if __name__ == "__main__":
    asyncio.run(etl_bonus_points_breakdown_flow())