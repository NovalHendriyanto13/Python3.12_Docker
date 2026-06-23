import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from configs.database import AsyncSessionLocal
from configs.app_config import mongo_uri, mongo_db
from app.services.mongo.offers_mongo import upsert_offers
from app.services.mongo.consumers_mongo import upsert_consumers
from app.services.mongo.advertisements_mongo import upsert_advertisements
from app.services.mongo.loyalty_points_card_transactions_mongo import upsert_loyalty_points_card_transactions
from app.services.mongo.loyalty_stamp_card_transactions_mongo import upsert_loyalty_stamp_card_transactions
from app.services.mongo.loyalty_stamp_card_reward_transactions_mongo import upsert_loyalty_stamp_card_reward_transactions
from app.services.mongo.sales_headers_mongo import upsert_sales_headers

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
    "mcd_loyalty_stamp_card_transactions": {
        "upsert": upsert_loyalty_stamp_card_transactions,
    },
    "mcd_loyalty_stamp_card_reward_transactions": {
        "upsert": upsert_loyalty_stamp_card_transactions,
    },
    "mcd_sales_headers": {
        "upsert": upsert_sales_headers,
    },
    "mcd_advertisement_activities": {
        "upsert": upsert_advertisement_activities,
    },
    "mcd_bonus_points_breakdown": {
        "upsert": upsert_bonus_points_breakdown,
    },
    "mcd_campaigns": {
        "upsert": upsert_campaigns,
    },
    "mcd_consumer_activities": {
        "upsert": upsert_consumer_activities,
    },
    "mcd_consumer_external_ids": {
        "upsert": upsert_consumer_external_ids,
    },
    "mcd_consumer_tags": {
        "upsert": upsert_consumer_tags,
    },
    "mcd_gdpr_consumer_consent_events": {
        "upsert": upsert_gdpr_consumer_consent_events,
    },
    "mcd_gdpr_consumer_consent_snapshot": {
        "upsert": upsert_gdpr_consumer_consent_snapshot,
    },
    "mcd_loyalty_activities": {
        "upsert": upsert_loyalty_activities,
    },
    "mcd_loyalty_program_rewards": {
        "upsert": upsert_loyalty_program_rewards,
    },
    "mcd_loyalty_programs": {
        "upsert": upsert_loyalty_programs,
    },
    "mcd_messages": {
        "upsert": upsert_messages,
    },
    "mcd_offer_activities": {
        "upsert": upsert_offer_activities,
    },
    "mcd_pushmessage_activities": {
        "upsert": upsert_pushmessage_activities,
    },
    "mcd_sale_details": {
        "upsert": upsert_sale_details,
    },
    "mcd_tag_values": {
        "upsert": upsert_tag_values,
    },
    "mcd_venues": {
        "upsert": upsert_venues,
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