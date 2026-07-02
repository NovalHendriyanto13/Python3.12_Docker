from motor.motor_asyncio import AsyncIOMotorClient
from configs.app_config import mongo_uri, mongo_db

client: AsyncIOMotorClient = None
db = None

async def connect_to_mongo():
    global client, db
    client = AsyncIOMotorClient(mongo_uri)
    db = client[mongo_db]


async def close_mongo_connection():
    global client
    client.close()

async def insert_mongo(collection: str, rows: list):
    return db[collection].insert_many(rows)
