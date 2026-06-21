import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from configs.database import AsyncSessionLocal
from configs.app_config import mongo_uri, mongo_db
from app.services.mongo.offers_mongo import upsert_offers
from app.services.mongo.consumers_mongo import upsert_consumers
from app.services.mongo.advertisements_mongo import upsert_advertisements
from app.services.mongo.loyalty_points_card_transactions_mongo import upsert_loyalty_points_card_transactions

COLLECTION_HANDLERS = {
    "mcd_offers": {
        "upsert": upsert_offers,
    },
    "mcd_consumer": {
        "upsert": upsert_consumers,
    },
    "mcd_advertisements": {
        "upsert": upsert_advertisements,
    },
    "mcd_loyalty_points_card_transactions": {
        "upsert": upsert_loyalty_points_card_transactions,
    },
}

async def watch_mongo(tablename: str):
    client = AsyncIOMotorClient(mongo_uri)
    collection = client[mongo_db][tablename]
    handlers = COLLECTION_HANDLERS[tablename]

    # Pipeline = filter, kita hanya mau dengerin 3 kejadian ini
    pipeline = [{
        "$match": {
            "operationType": {"$in": ["insert", "update", "replace"]}
        }
    }]

    while True:
        try:
            async with collection.watch(pipeline, full_document="updateLookup") as stream:
                async for change in stream:  # setiap ada perubahan masuk sini
                    op = change["operationType"]

                    async with AsyncSessionLocal() as session:
                        if op in ("insert", "update", "replace"):
                            # fullDocument = data lengkap yang berubah
                            await handlers["upsert"](session, change["fullDocument"])
        except Exception as e:
            import traceback
            print(f"❌ Error in watch_mongo for table {tablename}:")
            traceback.print_exc()
            await asyncio.sleep(5)