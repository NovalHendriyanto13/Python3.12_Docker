import asyncio
from app.listeners.mongo_listener import watch_mongo

WATCH_COLLECTIONS = [
    "mcd_advertisement_activities",
    "mcd_advertisements",
    "mcd_bonus_points_breakdown",
    "mcd_campaigns",
    "mcd_consumer_activities",
    "mcd_consumer_external_ids",
    "mcd_consumer_tags",
    "mcd_consumers",
    "mcd_gdpr_consumer_consent_events",
    "mcd_gdpr_consumer_consent_snapshot",
    "mcd_loyalty_activities",
    "mcd_loyalty_program_rewards",
    "mcd_loyalty_programs",
    "mcd_messages",
    "mcd_offer_activities",
    "mcd_offers",
    "mcd_points_program_transactions",
    "mcd_pushmessage_activities",
    "mcd_sale_details",
    "mcd_sale_headers",
    "mcd_stamp_program_reward_transactions",
    "mcd_stamp_program_transactions",
    "mcd_tag_values",
    "mcd_venues",

    "stg_data_changes"
]

async def start_tasks():
    tasks = [
        asyncio.create_task(watch_mongo(table_name))
        for table_name in WATCH_COLLECTIONS
    ]
    return tasks

async def stop_tasks(tasks):
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)