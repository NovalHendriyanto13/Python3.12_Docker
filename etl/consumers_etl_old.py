import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from configs.database import AsyncSessionLocal
from configs.app_config import mongo_uri, mongo_db
from app.services.mongo.consumers_mongo import upsert_consumers

@task(retries=3, retry_delay_seconds=10, log_prints=True)
async def extract_consumers() -> list[dict]:
    client = AsyncIOMotorClient(
        mongo_uri,
        serverSelectionTimeoutMS=30000,  # 30 detik
        connectTimeoutMS=30000,
        socketTimeoutMS=60000, 
    )
    db = client[mongo_db]

    batch_size = 10000
    skip = 0
    all_docs = []

    while True:
        batch = await db["mcd_consumer"].find().skip(skip).limit(batch_size).to_list(length=batch_size)
        if not batch:
            break
        all_docs.extend(batch)
        skip += batch_size
        print(f"Fetched {len(all_docs)} consumers so far...")

    return all_docs

@task(log_prints=True)
async def load_consumers(docs: list[dict]):
    async with AsyncSessionLocal() as session:
        for doc in docs:
            await upsert_consumers(session, doc)
    print(f"Loaded {len(docs)} documents to PostgreSQL")

@task(retries=3, retry_delay_seconds=10, log_prints=True)
async def extract_and_load_consumers():
    client = AsyncIOMotorClient(
        mongo_uri,
        serverSelectionTimeoutMS=30000,  # 30 detik
        connectTimeoutMS=30000,
        socketTimeoutMS=60000, 
    )
    db = client[mongo_db]

    batch_size = 1000000
    skip = 0
    total = 0

    while True:
        # Ambil per batch
        batch = await db["mcd_consumer"].find().skip(skip).limit(batch_size).to_list(length=batch_size)
        
        if not batch:
            break

        # Langsung load ke postgres per batch
        async with AsyncSessionLocal() as session:
            for doc in batch:
                await upsert_consumers(session, doc)

        total += len(batch)
        skip += batch_size
        print(f"✅ Processed {total} consumers...")

    print(f"✅ Total loaded: {total} consumers")

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_consumer_flow():
    print("ETL Flow Consumer started...")
    await extract_and_load_consumers()
    print("ETL Flow Consumer finished!")

if __name__ == "__main__":
    asyncio.run(etl_consumer_flow())