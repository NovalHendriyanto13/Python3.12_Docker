from motor.motor_asyncio import AsyncIOMotorClient
from configs.app_config import mongo_uri, mongo_db

mongo_client = AsyncIOMotorClient(mongo_uri)
mongo_conn = mongo_client[mongo_db]