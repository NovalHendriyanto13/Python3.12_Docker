from motor.motor_asyncio import AsyncIOMotorClient
from configs.app_config import mongo_uri, mongo_db, mongo_prefix
from typing import Union, List, Tuple
from pymongo import UpdateOne

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
    collection_table = f"{mongo_prefix}{collection}"

    return await db[collection_table].insert_many(rows)

async def upsert_mongo(collection: str, rows: list, unique_key: Union[str, List[str], Tuple[str, ...]]):
    collection_table = f"{mongo_prefix}{collection}"

    keys = [unique_key] if isinstance(unique_key, str) else list(unique_key)
    
    operations = []
    for row in rows:
        if not all(k in row for k in keys):
            continue
            
        filter_criteria = {k: row[k] for k in keys}
        
        operations.append(
            UpdateOne(filter_criteria, {"$set": row}, upsert=True)
        )
        
    if not operations:
        return None
        
    return await db[collection_table].bulk_write(operations)
