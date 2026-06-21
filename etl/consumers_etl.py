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
            text("CREATE TEMP TABLE temp_mcd_consumer (LIKE mcd_consumer EXCLUDING ALL) ON COMMIT DROP")
        )
        
        # Stream load to staging table
        await asyncpg_conn.copy_records_to_table(
            'temp_mcd_consumer', records=batch, columns=columns
        )
        
        # Merge/Upsert query
        upsert_query = """
        INSERT INTO mcd_consumer (
            reportingid, firstname, lastname, fullname, emailaddress,
            registrationtype, postcode, creationdate, modifieddate, dateofbirth,
            gender, isdeactivated, deactivationdate, lastknowndeviceid, phonenumber,
            consumertype
        )
        SELECT 
            reportingid, firstname, lastname, fullname, emailaddress,
            registrationtype, postcode, creationdate, modifieddate, dateofbirth,
            gender, isdeactivated, deactivationdate, lastknowndeviceid, phonenumber,
            consumertype
        FROM temp_mcd_consumer
        ON CONFLICT (reportingid) DO UPDATE SET
            firstname = EXCLUDED.firstname,
            lastname = EXCLUDED.lastname,
            fullname = EXCLUDED.fullname,
            emailaddress = EXCLUDED.emailaddress,
            registrationtype = EXCLUDED.registrationtype,
            postcode = EXCLUDED.postcode,
            creationdate = EXCLUDED.creationdate,
            modifieddate = EXCLUDED.modifieddate,
            dateofbirth = EXCLUDED.dateofbirth,
            gender = EXCLUDED.gender,
            isdeactivated = EXCLUDED.isdeactivated,
            deactivationdate = EXCLUDED.deactivationdate,
            lastknowndeviceid = EXCLUDED.lastknowndeviceid,
            phonenumber = EXCLUDED.phonenumber,
            consumertype = EXCLUDED.consumertype;
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
        "reportingid", "firstname", "lastname", "fullname", "emailaddress",
        "registrationtype", "postcode", "creationdate", "modifieddate", "dateofbirth",
        "gender", "isdeactivated", "deactivationdate", "lastknowndeviceid", "phonenumber",
        "consumertype"
    ]
    
    chunk_size = 1000000  # Batasi 1.000.000 data per transaksi database
    batch = []
    total = 0
    
    print("⏳ Streaming data from MongoDB and synchronizing in chunks of 1,000,000...")
    
    async for doc in cursor:
        row = (
            doc.get("reportingid"),
            doc.get("firstname"),
            doc.get("lastname"),
            doc.get("fullname"),
            doc.get("emailaddress"),
            doc.get("registrationtype"),
            doc.get("postcode"),
            doc.get("creationdate"),
            doc.get("modifieddate"),
            doc.get("dateofbirth"),
            doc.get("gender"),
            str(doc.get("isdeactivated")) if doc.get("isdeactivated") is not None else None,
            doc.get("deactivationdate"),
            doc.get("lastknowndeviceid"),
            doc.get("phonenumber"),
            doc.get("consumertype")
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