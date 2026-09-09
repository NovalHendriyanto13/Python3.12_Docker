import sys
import os
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_bool, to_decimal, to_jsonb_bool_map

# Helper function to load a single chunk to PostgreSQL using COPY + Merge
async def load_chunk_to_postgres(batch: list, columns: list):
    async with engine.begin() as conn:
        raw_conn = await conn.get_raw_connection()
        asyncpg_conn = raw_conn.driver_connection
        
        # Create staging table for this chunk
        await conn.execute(
            text("CREATE TEMP TABLE temp_mcd_gdpr_consumer_consent_snapshot (LIKE mcd_gdpr_consumer_consent_snapshot EXCLUDING ALL) ON COMMIT DROP")
        )
        
        # Stream load to staging table
        await asyncpg_conn.copy_records_to_table(
            'temp_mcd_gdpr_consumer_consent_snapshot', records=batch, columns=columns
        )
        
        # Merge/Upsert query
        upsert_query = """
        INSERT INTO mcd_gdpr_consumer_consent_snapshot (
            reporting_id,
            market,
            consent_to_store_and_process,
            services,
            event_time_utc,
            date
        )
        SELECT 
            reporting_id,
            market,
            consent_to_store_and_process,
            services,
            event_time_utc,
            date    
        FROM temp_mcd_gdpr_consumer_consent_snapshot
        ON CONFLICT (reporting_id) DO UPDATE SET
            market = EXCLUDED.market,
            consent_to_store_and_process = EXCLUDED.consent_to_store_and_process,
            services = EXCLUDED.services,
            event_time_utc = EXCLUDED.event_time_utc,
            date = EXCLUDED.date;
        """
        await conn.execute(text(upsert_query))

MONGO_PAGE_SIZE = 250000  # docs per Mongo connection "lease" - this collection has 12M+ docs;
                          # a single connection held open for all of it is prone to mid-stream
                          # resets (SSH tunnel / server-side idle kill), so each page opens its
                          # own short-lived client and resumes from the last _id seen.

@task(retries=5, retry_delay_seconds=15, log_prints=True)
async def fetch_gdpr_snapshot_page(last_id):
    client = AsyncIOMotorClient(
        mongo_uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=120000,
    )
    try:
        db = client[mongo_db]
        query = {"_id": {"$gt": last_id}} if last_id is not None else {}
        cursor = (
            db["mcd_gdpr_consumer_consent_event_log_snapshot"]
            .find(query)
            .sort("_id", 1)
            .limit(MONGO_PAGE_SIZE)
        )

        rows = []
        new_last_id = last_id
        async for doc in cursor:
            new_last_id = doc["_id"]
            rows.append((
                to_uuid(doc.get("reportingid")),
                doc.get("market"),
                to_bool(doc.get("consenttostoreandprocess")),
                to_jsonb_bool_map(doc.get("services")),
                to_datetime(doc.get("timestamp")),
                to_datetime(doc.get("date")),
            ))
        return rows, new_last_id
    finally:
        client.close()

@task(retries=1, retry_delay_seconds=10, log_prints=True)
async def extract_and_load_gdpr_consumer_consent_snapshot_fast():
    columns = [
        "reporting_id",
        "market",
        "consent_to_store_and_process",
        "services",
        "event_time_utc",
        "date"
    ]

    chunk_size = 250000  # kept modest since this runs alongside ~15 other concurrent flows
    pg_batch = []
    total = 0
    last_id = None

    print("⏳ Paging through MongoDB (250k docs/connection) and synchronizing in chunks of 1,000,000...")

    while True:
        rows, new_last_id = await fetch_gdpr_snapshot_page(last_id)
        if not rows:
            break

        pg_batch.extend(rows)
        last_id = new_last_id

        if len(pg_batch) >= chunk_size:
            await load_chunk_to_postgres(pg_batch, columns)
            total += len(pg_batch)
            print(f"   ✅ Synchronized chunk of {len(pg_batch)} gdpr_consumer_consent_snapshot (Total: {total})")
            pg_batch = []
            # Beri jeda ke event loop agar Prefect bisa mengirim heartbeat
            await asyncio.sleep(0.1)

        if len(rows) < MONGO_PAGE_SIZE:
            break  # last page

    if pg_batch:
        await load_chunk_to_postgres(pg_batch, columns)
        total += len(pg_batch)
        print(f"   ✅ Synchronized chunk of {len(pg_batch)} gdpr_consumer_consent_snapshot (Total: {total})")

    print(f"🚀 Bulk Sync Finished! Total {total} gdpr_consumer_consent_snapshot successfully upserted.")

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_gdpr_consumer_consent_snapshot_flow():
    print("ETL Flow gdpr_consumer_consent_snapshot started...")
    await extract_and_load_gdpr_consumer_consent_snapshot_fast()
    print("ETL Flow gdpr_consumer_consent_snapshot finished!")

if __name__ == "__main__":
    asyncio.run(etl_gdpr_consumer_consent_snapshot_flow())