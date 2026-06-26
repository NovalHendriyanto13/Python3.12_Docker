import sys, os, asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool, to_uuid

TABLE_NAME="loyalty_programs"
TARGET_TABLE="loyalty_programs"

async def load_chunk_to_postgres(batch:list, columns:list):
    async with engine.begin() as conn:
        raw_conn=await conn.get_raw_connection()
        asyncpg_conn=raw_conn.driver_connection
        await conn.execute(text("CREATE TEMP TABLE temp_loyalty_programs (LIKE loyalty_programs EXCLUDING ALL) ON COMMIT DROP"))
        await asyncpg_conn.copy_records_to_table('temp_loyalty_programs', records=batch, columns=columns)
        upsert_query="""
        INSERT INTO loyalty_programs (
            loyalty_program_id,
            campaign_id,
            category_id,
            max_instances,
            extended_data,
            name,
            title,
            sub_title,
            description,
            instructions,
            status,
            terms_and_conditions,
            points_required,
            days_of_week,
            weighting,
            daily_start_time,
            daily_end_time,
            max_points_per_day,
            apply_initial_points_to_subsequent_cards,
            max_points_requests_per_day,
            initial_points,
            is_hidden,
            require_ip_whitelisting,
            loyalty_program_type,
            points_expiry_days,
            expiry_schedule_details,
            is_consumer_api_write_accessible,
            market,
            start_date,
            end_date,
            when_last_updated
        )
        SELECT 
            loyalty_program_id,
            campaign_id,
            category_id,
            max_instances,
            extended_data,
            name,
            title,
            sub_title,
            description,
            instructions,
            status,
            terms_and_conditions,
            points_required,
            days_of_week,
            weighting,
            daily_start_time,
            daily_end_time,
            max_points_per_day,
            apply_initial_points_to_subsequent_cards,
            max_points_requests_per_day,
            initial_points,
            is_hidden,
            require_ip_whitelisting,
            loyalty_program_type,
            points_expiry_days,
            expiry_schedule_details,
            is_consumer_api_write_accessible,
            market,
            start_date,
            end_date,
            when_last_updated FROM temp_loyalty_programs
        ON CONFLICT (loyalty_program_id) DO UPDATE SET
            campaign_id=EXCLUDED.campaign_id,
            category_id=EXCLUDED.category_id,
            max_instances=EXCLUDED.max_instances,
            extended_data=EXCLUDED.extended_data,
            name=EXCLUDED.name,
            title=EXCLUDED.title,
            sub_title=EXCLUDED.sub_title,
            description=EXCLUDED.description,
            instructions=EXCLUDED.instructions,
            status=EXCLUDED.status,
            terms_and_conditions=EXCLUDED.terms_and_conditions,
            points_required=EXCLUDED.points_required,
            days_of_week=EXCLUDED.days_of_week,
            weighting=EXCLUDED.weighting,
            daily_start_time=EXCLUDED.daily_start_time,
            daily_end_time=EXCLUDED.daily_end_time,
            max_points_per_day=EXCLUDED.max_points_per_day,
            apply_initial_points_to_subsequent_cards=EXCLUDED.apply_initial_points_to_subsequent_cards,
            max_points_requests_per_day=EXCLUDED.max_points_requests_per_day,
            initial_points=EXCLUDED.initial_points,
            is_hidden=EXCLUDED.is_hidden,
            require_ip_whitelisting=EXCLUDED.require_ip_whitelisting,
            loyalty_program_type=EXCLUDED.loyalty_program_type,
            points_expiry_days=EXCLUDED.points_expiry_days,
            expiry_schedule_details=EXCLUDED.expiry_schedule_details,
            is_consumer_api_write_accessible=EXCLUDED.is_consumer_api_write_accessible,
            market=EXCLUDED.market,
            start_date=EXCLUDED.start_date,
            end_date=EXCLUDED.end_date,
            when_last_updated=EXCLUDED.when_last_updated;
        """
        await conn.execute(text(upsert_query))

@task(retries=3,retry_delay_seconds=10,log_prints=True)
async def extract_and_load():
    client=AsyncIOMotorClient(mongo_uri)
    db=client[mongo_db]
    cursor=db["loyalty_programs"].find().batch_size(100000)
    columns=[
        'loyalty_program_id', 
        'campaign_id', 
        'category_id', 
        'max_instances', 
        'extended_data', 
        'name', 
        'title', 
        'sub_title', 
        'description', 
        'instructions', 
        'status', 
        'terms_and_conditions', 
        'points_required', 
        'days_of_week', 
        'weighting', 
        'daily_start_time', 
        'daily_end_time', 
        'max_points_per_day', 
        'apply_initial_points_to_subsequent_cards', 
        'max_points_requests_per_day', 
        'initial_points', 
        'is_hidden', 
        'require_ip_whitelisting', 
        'loyalty_program_type', 
        'points_expiry_days', 
        'expiry_schedule_details', 
        'is_consumer_api_write_accessible', 
        'market', 
        'start_date', 
        'end_date', 
        'when_last_updated'
    ]
    batch=[]
    chunk_size=1000000
    total=0
    async for doc in cursor:
        row=(
            to_int(doc.get("loyaltyprogramid")),
            to_int(doc.get("campaignid")),
            to_int(doc.get("categoryid")),
            to_int(doc.get("maxinstances")),
            doc.get("extendeddata"),
            doc.get("name"),
            doc.get("title"),
            doc.get("subtitle"),
            doc.get("description"),
            doc.get("instructions"),
            to_int(doc.get("status")),
            doc.get("termsandconditions"),
            to_int(doc.get("pointsrequired")),
            doc.get("daysofweek"),
            to_int(doc.get("weighting")),
            to_int(doc.get("dailystarttime")),
            to_int(doc.get("dailyendtime")),
            to_int(doc.get("maxpointsperday")),
            to_bool(doc.get("applyinitialpointstosubsequentcards")),
            to_int(doc.get("maxpointsrequestsperday")),
            to_int(doc.get("initialpoints")),
            to_bool(doc.get("ishidden")),
            to_bool(doc.get("requireipwhitelisting")),
            to_int(doc.get("loyaltyprogramtype")),
            to_int(doc.get("pointsexpirydays")),
            doc.get("expiryscheduledetails"),
            to_bool(doc.get("isconsumerapiwriteaccessible")),
            doc.get("market"),
            to_datetime(doc.get("startdate")),
            to_datetime(doc.get("enddate")),
            to_datetime(doc.get("whenlastupdated"))
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

@flow(name="loyalty_programs-etl")
async def etl_flow():
    await extract_and_load()

if __name__=="__main__":
    asyncio.run(etl_flow())
