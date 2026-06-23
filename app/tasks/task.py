import asyncio
from app.listeners.mongo_listener import watch_mongo

WATCH_COLLECTIONS = [
    "mcd_offers",
    "mcd_consumer",
    "mcd_advertisements",
    "mcd_loyalty_points_card_transactions",
    "mcd_loyalty_stamp_card_transactions",
    "mcd_loyalty_stamp_card_reward_transactions",
    "mcd_sales_headers"
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