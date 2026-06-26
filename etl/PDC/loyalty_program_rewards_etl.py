import sys, os, asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool, to_uuid

TABLE_NAME="loyalty_program_rewards"
TARGET_TABLE="loyalty_program_rewards"

async def load_chunk_to_postgres(batch:list, columns:list):
    async with engine.begin() as conn:
        raw_conn=await conn.get_raw_connection()
        asyncpg_conn=raw_conn.driver_connection
        await conn.execute(text("CREATE TEMP TABLE temp_loyalty_program_rewards (LIKE loyalty_program_rewards EXCLUDING ALL) ON COMMIT DROP"))
        await asyncpg_conn.copy_records_to_table('temp_loyalty_program_rewards', records=batch, columns=columns)
        upsert_query="""
        INSERT INTO loyalty_program_rewards (
            id,
            loyalty_program_id,
            reward_type,
            offer_id,
            expires_after_n_days,
            is_expiry_time_specified,
            expiry_time_after_activation,
            point_value,
            activation_limit,
            activation_weight,
            activation_remained,
            market
        )
        SELECT 
            id,
            loyalty_program_id,
            reward_type,
            offer_id,
            expires_after_n_days,
            is_expiry_time_specified,
            expiry_time_after_activation,
            point_value,
            activation_limit,
            activation_weight,
            activation_remained,
            market 
        FROM temp_loyalty_program_rewards
        ON CONFLICT (id) DO UPDATE SET
            loyalty_program_id=EXCLUDED.loyalty_program_id,
            reward_type=EXCLUDED.reward_type,
            offer_id=EXCLUDED.offer_id,
            expires_after_n_days=EXCLUDED.expires_after_n_days,
            is_expiry_time_specified=EXCLUDED.is_expiry_time_specified,
            expiry_time_after_activation=EXCLUDED.expiry_time_after_activation,
            point_value=EXCLUDED.point_value,
            activation_limit=EXCLUDED.activation_limit,
            activation_weight=EXCLUDED.activation_weight,
            activation_remained=EXCLUDED.activation_remained,
            market=EXCLUDED.market;
        """
        await conn.execute(text(upsert_query))

@task(retries=3,retry_delay_seconds=10,log_prints=True)
async def extract_and_load():
    client=AsyncIOMotorClient(mongo_uri)
    db=client[mongo_db]
    cursor=db["loyalty_program_rewards"].find().batch_size(100000)
    columns=[
        'id', 
        'loyalty_program_id', 
        'reward_type', 
        'offer_id', 
        'expires_after_n_days', 
        'is_expiry_time_specified', 
        'expiry_time_after_activation', 
        'point_value', 
        'activation_limit', 
        'activation_weight', 
        'activation_remained', 
        'market'
    ]
    batch=[]
    chunk_size=1000000
    total=0
    async for doc in cursor:
        row=(
            to_int(doc.get("id")),
            to_int(doc.get("loyalty_program_id")),
            to_int(doc.get("reward_type")),
            to_int(doc.get("offer_id")),
            to_int(doc.get("expires_after_n_days")),
            to_bool(doc.get("is_expiry_time_specified")),
            to_int(doc.get("expiry_time_after_activation")),
            to_int(doc.get("point_value")),
            to_int(doc.get("activation_limit")),
            to_int(doc.get("activation_weight")),
            to_int(doc.get("activation_remained")),
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

@flow(name="loyalty_program_rewards-etl")
async def etl_loyalty_program_rewards_flow():
    await extract_and_load()

if __name__=="__main__":
    asyncio.run(etl_loyalty_program_rewards_flow())
