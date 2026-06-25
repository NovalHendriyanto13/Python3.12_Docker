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

# Helper function to load a single chunk to PostgreSQL using COPY + Merge
async def load_chunk_to_postgres(batch: list, columns: list):
    async with engine.begin() as conn:
        raw_conn = await conn.get_raw_connection()
        asyncpg_conn = raw_conn.driver_connection
        
        # Create staging table for this chunk
        await conn.execute(
            text("CREATE TEMP TABLE temp_mcd_consumer (LIKE mcd_consumer EXCLUDING ALL) ON COMMIT DROP")
        )
        
        # Stream load to staging table
        await asyncpg_conn.copy_records_to_table(
            'temp_mcd_consumer', records=batch, columns=columns
        )
        
        # Merge/Upsert query
        upsert_query = """
        INSERT INTO mcd_consumer (
            reporting_id,
            last_known_device_id,
            market,
            first_name,
            last_name,
            full_name,
            email_address,
            gender,
            date_of_birth,
            phone_number,
            postcode,
            is_deactivated,
            registration_type,
            consumer_type,
            registration_source,
            creation_date,
            modified_date,
            deactivation_date
        )
        SELECT 
            reporting_id,
            last_known_device_id,
            market,
            first_name,
            last_name,
            full_name,
            email_address,
            gender,
            date_of_birth,
            phone_number,
            postcode,
            is_deactivated,
            registration_type,
            consumer_type,
            registration_source,
            creation_date,
            modified_date,
            deactivation_date
        FROM temp_mcd_consumer
        ON CONFLICT (reporting_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            full_name = EXCLUDED.full_name,
            email_address = EXCLUDED.email_address,
            registration_type = EXCLUDED.registration_type,
            postcode = EXCLUDED.postcode,
            creation_date = EXCLUDED.creation_date,
            modified_date = EXCLUDED.modified_date,
            date_of_birth = EXCLUDED.date_of_birth,
            gender = EXCLUDED.gender,
            is_deactivated = EXCLUDED.is_deactivated,
            deactivation_date = EXCLUDED.deactivation_date,
            last_known_device_id = EXCLUDED.last_known_device_id,
            registration_source = EXCLUDED.registration_source,
            phone_number = EXCLUDED.phone_number,
            consumer_type = EXCLUDED.consumer_type;
        """
        await conn.execute(text(upsert_query))

@task(retries=3, retry_delay_seconds=10, log_prints=True)
async def extract_and_load_consumers_fast():
    client = AsyncIOMotorClient(
        mongo_uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000, 
    )
    db = client[mongo_db]
    
    # Stream from MongoDB using cursor
    cursor = db["mcd_consumer"].find().batch_size(100000)
    
    columns = [
        "reporting_id",
        "last_known_device_id",
        "market",
        "first_name",
        "last_name",
        "full_name",
        "email_address",
        "gender",
        "date_of_birth",
        "phone_number",
        "postcode",
        "is_deactivated",
        "registration_type",
        "consumer_type",
        "registration_source",
        "creation_date",
        "modified_date",
        "deactivation_date"
    ]
    
    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0
    
    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")
    
    async for doc in cursor:
        row = (
            to_uuid(doc.get("reportingid")),
            doc.get("lastknowndeviceid"),
            doc.get("market"),
            doc.get("firstname"),
            doc.get("lastname"),
            doc.get("fullname"),
            doc.get("emailaddress"),
            doc.get("gender"),
            to_datetime(doc.get("dateofbirth")),
            doc.get("phonenumber"),
            doc.get("postcode"),
            to_bool(doc.get("isdeactivated")),
            to_int(doc.get("registrationtype")),
            to_int(doc.get("consumertype")),
            doc.get("registration_source"),
            to_datetime(doc.get("creationdate")),
            to_datetime(doc.get("modifieddate")),
            to_datetime(doc.get("deactivationdate"))
        )
        batch.append(row)
        
        if len(batch) >= chunk_size:
            await load_chunk_to_postgres(batch, columns)
            total += len(batch)
            print(f"   ✅ Synchronized chunk of {len(batch)} consumers (Total: {total})")
            batch = []
            # Beri jeda ke event loop agar Prefect bisa mengirim heartbeat
            await asyncio.sleep(0.1)
            
    if batch:
        await load_chunk_to_postgres(batch, columns)
        total += len(batch)
        print(f"   ✅ Synchronized chunk of {len(batch)} consumers (Total: {total})")
        
    print(f"🚀 Bulk Sync Finished! Total {total} consumers successfully upserted.")

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_consumer_flow():
    print("ETL Flow Consumer started...")
    await extract_and_load_consumers_fast()
    print("ETL Flow Consumer finished!")

if __name__ == "__main__":
    asyncio.run(etl_consumer_flow())