from motor.motor_asyncio import AsyncIOMotorClient
from configs.app_config import mongo_uri, mongo_db

mongo_client = AsyncIOMotorClient(
    mongo_uri,
    serverSelectionTimeoutMS=10000,
    connectTimeoutMS=10000,
)
mongo_conn = mongo_client[mongo_db]