import sys, os, asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool, to_uuid, to_decimal

TABLE_NAME="mcd_offers"
TARGET_TABLE="mcd_offers"

async def load_chunk_to_postgres(batch:list, columns:list):
    async with engine.begin() as conn:
        raw_conn=await conn.get_raw_connection()
        asyncpg_conn=raw_conn.driver_connection
        await conn.execute(text("CREATE TEMP TABLE temp_mcd_offers (LIKE mcd_offers EXCLUDING ALL) ON COMMIT DROP"))
        await asyncpg_conn.copy_records_to_table('temp_mcd_offers', records=batch, columns=columns)
        upsert_query="""
        INSERT INTO mcd_offers (
            offer_id,
            campaign_id,
            category_id,
            category,
            market,
            title,
            description,
            terms_and_conditions,
            has_barcode_image,
            redemption_limit,
            payment_type,
            redemption_count_unlimited,
            code_type,
            discount_percent,
            discount_value,
            status,
            apply_geo_fence_filters,
            base_weight,
            no_compete_group,
            is_giftable,
            is_reward,
            is_respawning,
            respawns_in_days,
            enable_distance_weight,
            is_available_all_stores,
            promotional_image_description,
            limit,
            code_expiry_in_minutes,
            is_sticky,
            sticky_expiration_days,
            sticky_expiration_time_of_day,
            offer_type,
            respawn_start_time,
            name,
            respawns_in_minutes,
            consumer_redemption_limit,
            redemption_text,
            days_of_week,
            daily_start_time,
            daily_end_time,
            offer_start_time,
            offer_expire_time,
            when_last_updated_utc
      
        )
        SELECT 
            offer_id,
            campaign_id,
            category_id,
            category,
            market,
            title,
            description,
            terms_and_conditions,
            has_barcode_image,
            redemption_limit,
            payment_type,
            redemption_count_unlimited,
            code_type,
            discount_percent,
            discount_value,
            status,
            apply_geo_fence_filters,
            base_weight,
            no_compete_group,
            is_giftable,
            is_reward,
            is_respawning,
            respawns_in_days,
            enable_distance_weight,
            is_available_all_stores,
            promotional_image_description,
            limit,
            code_expiry_in_minutes,
            is_sticky,
            sticky_expiration_days,
            sticky_expiration_time_of_day,
            offer_type,
            respawn_start_time,
            name,
            respawns_in_minutes,
            consumer_redemption_limit,
            redemption_text,
            days_of_week,
            daily_start_time,
            daily_end_time,
            offer_start_time,
            offer_expire_time,
            when_last_updated_utc 
        FROM temp_offers
        ON CONFLICT (offer_id) DO UPDATE SET
            offer_id = EXCLUDED.offer_id,
            campaign_id = EXCLUDED.campaign_id,
            category_id = EXCLUDED.category_id,
            category = EXCLUDED.category,
            market = EXCLUDED.market,
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            terms_and_conditions = EXCLUDED.terms_and_conditions,
            has_barcode_image = EXCLUDED.has_barcode_image,
            redemption_limit = EXCLUDED.redemption_limit,
            payment_type = EXCLUDED.payment_type,
            redemption_count_unlimited = EXCLUDED.redemption_count_unlimited,
            code_type = EXCLUDED.code_type,
            discount_percent = EXCLUDED.discount_percent,
            discount_value = EXCLUDED.discount_value,
            status = EXCLUDED.status,
            apply_geo_fence_filters = EXCLUDED.apply_geo_fence_filters,
            base_weight = EXCLUDED.base_weight,
            no_compete_group = EXCLUDED.no_compete_group,
            is_giftable = EXCLUDED.is_giftable,
            is_reward = EXCLUDED.is_reward,
            is_respawning = EXCLUDED.is_respawning,
            respawns_in_days = EXCLUDED.respawns_in_days,
            enable_distance_weight = EXCLUDED.enable_distance_weight,
            is_available_all_stores = EXCLUDED.is_available_all_stores,
            promotional_image_description = EXCLUDED.promotional_image_description,
            limit = EXCLUDED.limit,
            code_expiry_in_minutes = EXCLUDED.code_expiry_in_minutes,
            is_sticky = EXCLUDED.is_sticky,
            sticky_expiration_days = EXCLUDED.sticky_expiration_days,
            sticky_expiration_time_of_day = EXCLUDED.sticky_expiration_time_of_day,
            offer_type = EXCLUDED.offer_type,
            respawn_start_time = EXCLUDED.respawn_start_time,
            name = EXCLUDED.name,
            respawns_in_minutes = EXCLUDED.respawns_in_minutes,
            consumer_redemption_limit = EXCLUDED.consumer_redemption_limit,
            redemption_text = EXCLUDED.redemption_text,
            days_of_week = EXCLUDED.days_of_week,
            daily_start_time = EXCLUDED.daily_start_time,
            daily_end_time = EXCLUDED.daily_end_time,
            offer_start_time = EXCLUDED.offer_start_time,
            offer_expire_time = EXCLUDED.offer_expire_time,
            when_last_updated_utc = EXCLUDED.when_last_updated_utc;
        """
        await conn.execute(text(upsert_query))

@task(retries=3,retry_delay_seconds=10,log_prints=True)
async def extract_and_load():
    client=AsyncIOMotorClient(mongo_uri)
    db=client[mongo_db]
    cursor=db["mcd_offers"].find().batch_size(100000)
    columns=[
        'offer_id',
        'campaign_id',
        'category_id',
        'category',
        'market',
        'title',
        'description',
        'terms_and_conditions',
        'has_barcode_image',
        'redemption_limit',
        'payment_type',
        'redemption_count_unlimited',
        'code_type',
        'discount_percent',
        'discount_value',
        'status',
        'apply_geo_fence_filters',
        'base_weight',
        'no_compete_group',
        'is_giftable',
        'is_reward',
        'is_respawning',
        'respawns_in_days',
        'enable_distance_weight',
        'is_available_all_stores',
        'promotional_image_description',
        'limit',
        'code_expiry_in_minutes',
        'is_sticky',
        'sticky_expiration_days',
        'sticky_expiration_time_of_day',
        'offer_type',
        'respawn_start_time',
        'name',
        'respawns_in_minutes',
        'consumer_redemption_limit',
        'redemption_text',
        'days_of_week',
        'daily_start_time',
        'daily_end_time',
        'offer_start_time',
        'offer_expire_time',
        'when_last_updated_utc'
    ]
    batch=[]
    chunk_size=1000000
    total=0
    async for doc in cursor:
        row=(
            to_int(doc.get('offer_id')),
            to_int(doc.get('campaign_id')),
            to_int(doc.get('category_id')),
            (doc.get('category')),
            (doc.get('market')),
            (doc.get('title')),
            (doc.get('description')),
            (doc.get('terms_and_conditions')),
            to_bool(doc.get('has_barcode_image')),
            to_int(doc.get('redemption_limit')),
            to_int(doc.get('payment_type')),
            to_bool(doc.get('redemption_count_unlimited')),
            (doc.get('code_type')),
            to_decimal(doc.get('discount_percent')),
            to_decimal(doc.get('discount_value')),
            to_int(doc.get('status')),
            to_bool(doc.get('apply_geo_fence_filters')),
            to_int(doc.get('base_weight')),
            (doc.get('no_compete_group')),
            (doc.get('is_giftable')),
            to_bool(doc.get('is_reward')),
            to_bool(doc.get('is_respawning')),
            to_bool(doc.get('respawns_in_days')),
            (doc.get('enable_distance_weight')),
            (doc.get('is_available_all_stores')),
            (doc.get('promotional_image_description')),
            to_int(doc.get('limit')),
            to_int(doc.get('code_expiry_in_minutes')),
            to_bool(doc.get('is_sticky')),
            to_int(doc.get('sticky_expiration_days')),
            to_int(doc.get('sticky_expiration_time_of_day')),
            (doc.get('offer_type')),
            to_int(doc.get('respawn_start_time')),
            (doc.get('name')),
            to_int(doc.get('respawns_in_minutes')),
            to_int(doc.get('consumer_redemption_limit')),
            (doc.get('redemption_text')),
            (doc.get('days_of_week')),
            to_int(doc.get('daily_start_time')),
            to_int(doc.get('daily_end_time')),
            to_datetime(doc.get('offer_start_time')),
            to_datetime(doc.get('offer_expire_time')),
            to_datetime(doc.get('when_last_updated_utc'))
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

@flow(name="offers-etl")
async def etl_offers_flow():
    await extract_and_load()

if __name__=="__main__":
    asyncio.run(etl_offers_flow())
