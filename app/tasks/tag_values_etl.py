import sys, os, asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool, to_uuid

TABLE_NAME="tag_values"
TARGET_TABLE="tag_values"

async def load_chunk_to_postgres(batch:list, columns:list):
    async with engine.begin() as conn:
        raw_conn=await conn.get_raw_connection()
        asyncpg_conn=raw_conn.driver_connection
        await conn.execute(text("CREATE TEMP TABLE temp_tag_values (LIKE tag_values EXCLUDING ALL) ON COMMIT DROP"))
        await asyncpg_conn.copy_records_to_table('temp_tag_values', records=batch, columns=columns)
        upsert_query="""
        INSERT INTO tag_values (
            tag_value_id,
            name,
            reference_code,
            consumer_visible,
            consumer_updateable,
            market)
            SELECT tag_value_id,
            name,
            reference_code,
            consumer_visible,
            consumer_updateable,
            market 
        FROM temp_tag_values
        ON CONFLICT (tag_value_id) DO UPDATE SET
            name=EXCLUDED.name,
            reference_code=EXCLUDED.reference_code,
            consumer_visible=EXCLUDED.consumer_visible,
            consumer_updateable=EXCLUDED.consumer_updateable,
            market=EXCLUDED.market;
        """
        await conn.execute(text(upsert_query))

@task(retries=3,retry_delay_seconds=10,log_prints=True)
async def extract_and_load():
    client=AsyncIOMotorClient(mongo_uri)
    db=client[mongo_db]
    cursor=db["tag_values"].find().batch_size(100000)
    columns=[
        'tag_value_id', 
        'name', 
        'reference_code', 
        'consumer_visible', 
        'consumer_updateable', 
        'market'
    ]
    batch=[]
    chunk_size=1000000
    total=0
    async for doc in cursor:
        row=(
            to_uuid(doc.get("tag_value_id")),
            doc.get("name"),
            doc.get("reference_code"),
            to_bool(doc.get("consumer_visible")),
            to_bool(doc.get("consumer_updateable")),
            doc.get("market")
        )
        batch.append(row)
        if len(batch)>=chunk_size:
            await load_chunk_to_postgres(batch,columns)
            total+=len(batch)
            batch=[]
            await asyncio.sleep(0.1)
    if batch:
        await load_chunk_to_postgres(batch,columns)
        total+=len(batch)
    print(f"Finished sync {total} rows")

@flow(name="tag_values-etl")
async def etl_tag_values_flow():
    await extract_and_load()

if __name__=="__main__":
    asyncio.run(etl_tag_values_flow())
