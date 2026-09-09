import sys
import os
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool, to_uuid, to_string_list
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
            pos_sales_transaction_id = EXCLUDED.pos_sales_transaction_id,
            ordering_method = EXCLUDED.ordering_method,
            day_of_week = EXCLUDED.day_of_week,
            time_of_day = EXCLUDED.time_of_day,
            minimum_spend = EXCLUDED.minimum_spend,
            product = EXCLUDED.product,
            satisfied_condition_type = EXCLUDED.satisfied_condition_type,
            bonus_rule_id = EXCLUDED.bonus_rule_id,
            bonus_points = EXCLUDED.bonus_points,
            standard_points = EXCLUDED.standard_points,
            venue_id = EXCLUDED.venue_id,
            venue_name = EXCLUDED.venue_name,
            venue_external_id = EXCLUDED.venue_external_id,
            market = EXCLUDED.market,
            reporting_id = EXCLUDED.reporting_id,
            date = EXCLUDED.date,
            transaction_source_time_local = EXCLUDED.transaction_source_time_local,
            transaction_time_utc = EXCLUDED.transaction_time_utc,
            bonus_points_breakdown_position = EXCLUDED.bonus_points_breakdown_position;
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
    cursor = db["mcd_loyalty_points_bonus_breakdown_details"].find().batch_size(100000)

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
        "bonus_points_breakdown_position",
    ]

    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0

    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")

    # Real docs use already-underscored field names matching the columns above,
    # except ordering_method/satisfied_condition_type which are mangled JSON-array
    # strings, and minimum_spend/product/day_of_week/time_of_day/market/date/
    # bonus_points_breakdown_position which have no source data at all.
    async for doc in cursor:
        row = (
            to_uuid(doc.get("points_card_transaction_id")),
            doc.get("pos_sales_transaction_id"),
            to_string_list(doc.get("ordering_method")),
            to_string_list(doc.get("day_of_week")),
            to_string_list(doc.get("time_of_day")),
            None,
            None,
            to_string_list(doc.get("satisfied_condition_type")),
            doc.get("bonus_rule_id"),
            to_int(doc.get("bonus_points")),
            to_int(doc.get("standard_points")),
            to_int(doc.get("venue_id")),
            doc.get("venue_name"),
            doc.get("venue_external_id"),
            None,
            to_uuid(doc.get("reporting_id")),
            None,
            to_datetime(doc.get("transaction_source_time_local")),
            to_datetime(doc.get("transaction_time_utc")),
            None,
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