import asyncio
from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from configs.database import AsyncSessionLocal
from configs.app_config import mongo_uri, mongo_db
from app.services.mongo.offers_mongo import upsert_offers

@task(retries=3, retry_delay_seconds=10, log_prints=True)
async def extract_offers() -> list[dict]:
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
        batch = await db["mcd_offers"].find().skip(skip).limit(batch_size).to_list(length=batch_size)
        if not batch:
            break
        all_docs.extend(batch)
        skip += batch_size
        print(f"Fetched {len(all_docs)} offers so far...")

    return all_docs

@task(log_prints=True)
async def load_offers(docs: list[dict]):
    async with AsyncSessionLocal() as session:
        for doc in docs:
            await upsert_offers(session, doc)
    print(f"Loaded {len(docs)} documents to PostgreSQL")

@flow(name="mongo-to-postgres-etl", log_prints=True)
async def etl_offers_flow():
    print("ETL Flow started...")
    docs = await extract_offers()
    await load_offers(docs)
    print("ETL Flow finished!")

if __name__ == "__main__":
    asyncio.run(etl_offers_flow())