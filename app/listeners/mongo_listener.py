import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from configs.database import AsyncSessionLocal
from configs.app_config import mongo_uri, mongo_db

from app.services.mongo.advertisements_activities_mongo import upsert_advertisement_activities
from app.services.mongo.advertisements_mongo import upsert_advertisements
from app.services.mongo.bonus_points_breakdown_mongo import upsert_bonus_points_breakdown
from app.services.mongo.campaigns_mongo import upsert_campaigns
from app.services.mongo.consumer_activities_mongo import upsert_consumer_activities
from app.services.mongo.consumer_external_ids_mongo import upsert_consumer_external_ids
from app.services.mongo.consumer_tags_mongo import upsert_consumer_tags
from app.services.mongo.consumers_mongo import upsert_consumers
from app.services.mongo.gdpr_consumer_consent_events_mongo import upsert_gdpr_consumer_consent_events
from app.services.mongo.gdpr_consumer_consent_snapshot_mongo import upsert_gdpr_consumer_consent_snapshot
from app.services.mongo.loyalty_activities_mongo import upsert_loyalty_activities
from app.services.mongo.loyalty_program_rewards_mongo import upsert_loyalty_program_rewards
from app.services.mongo.loyalty_programs_mongo import upsert_loyalty_programs
from app.services.mongo.messages_mongo import upsert_messages
from app.services.mongo.offer_activities_mongo import upsert_offer_activities
from app.services.mongo.offers_mongo import upsert_offers
from app.services.mongo.points_program_transactions_mongo import upsert_points_program_transactions
from app.services.mongo.product_mongo import upsert_products
from app.services.mongo.pushmessage_activities_mongo import upsert_pushmessage_activities
from app.services.mongo.sale_details_mongo import upsert_sale_details
from app.services.mongo.sale_headers_mongo import upsert_sale_headers
from app.services.mongo.stamp_program_reward_transactions_mongo import upsert_stamp_program_reward_transactions
from app.services.mongo.stamp_program_transactions_mongo import upsert_stamp_program_transactions
from app.services.mongo.tag_values_mongo import upsert_tag_values
from app.services.mongo.venues_mongo import upsert_venues
from app.services.mongo.stg_data_changes_mongo import upsert_stg_data_changes

from app.services.mongo.sales_mongo import upsert_sales

COLLECTION_HANDLERS = {
    "mcd_advertisement_activities": {
        "upsert": upsert_advertisement_activities
    },
    "mcd_advertisements": {
        "upsert": upsert_advertisements
    },
    "mcd_bonus_points_breakdown": {
        "upsert": upsert_bonus_points_breakdown
    },
    "mcd_campaigns": {
        "upsert": upsert_campaigns
    },
    "mcd_consumer_activities": {
        "upsert": upsert_consumer_activities
    },
    "mcd_consumer_external_ids": {
        "upsert": upsert_consumer_external_ids
    },
    "mcd_consumer_tags": {
        "upsert": upsert_consumer_tags
    },
    "mcd_consumers": {
        "upsert": upsert_consumers
    },
    "mcd_gdpr_consumer_consent_events": {
        "upsert": upsert_gdpr_consumer_consent_events
    },
    "mcd_gdpr_consumer_consent_snapshot": {
        "upsert": upsert_gdpr_consumer_consent_snapshot
    },
    "mcd_loyalty_activities": {
        "upsert": upsert_loyalty_activities
    },
    "mcd_loyalty_program_rewards": {
        "upsert": upsert_loyalty_program_rewards
    },
    "mcd_loyalty_programs": {
        "upsert": upsert_loyalty_programs
    },
    "mcd_messages": {
        "upsert": upsert_messages
    },
    "mcd_offer_activities": {
        "upsert": upsert_offer_activities
    },
    "mcd_offers": {
        "upsert": upsert_offers
    },
    "mcd_points_program_transactions": {
        "upsert": upsert_points_program_transactions
    },
    "mcd_products": {
        "upsert": upsert_products
    },
    "mcd_pushmessage_activities": {
        "upsert": upsert_pushmessage_activities
    },
    "mcd_sale_details": {
        "upsert": upsert_sale_details
    },
    "mcd_sale_headers": {
        "upsert": upsert_sale_headers
    },
    "mcd_stamp_program_reward_transactions": {
        "upsert": upsert_stamp_program_reward_transactions
    },
    "mcd_stamp_program_transactions": {
        "upsert": upsert_stamp_program_transactions
    },
    "mcd_tag_values": {
        "upsert": upsert_tag_values
    },
    "mcd_venues": {
        "upsert": upsert_venues
    },
    # changes
    "stg_data_changes": {
        "upsert": upsert_stg_data_changes
    }
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