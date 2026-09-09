import sys, os, asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool, to_uuid

TABLE_NAME="mcd_venues"
TARGET_TABLE="mcd_venues"

async def load_chunk_to_postgres(batch:list, columns:list):
    async with engine.begin() as conn:
        raw_conn=await conn.get_raw_connection()
        asyncpg_conn=raw_conn.driver_connection
        await conn.execute(text("CREATE TEMP TABLE temp_mcd_venues (LIKE mcd_venues EXCLUDING ALL) ON COMMIT DROP"))
        await asyncpg_conn.copy_records_to_table('temp_mcd_venues', records=batch, columns=columns)
        upsert_query="""
        INSERT INTO mcd_venues (
            venue_id,
            venue_external_id,
            region_id,
            market,
            name,
            location_lat,
            location_long,
            address_line1,
            address_line2,
            address_line3,
            post_code,
            venue_type_code,
            is_hidden,
            region,
            features,
            extended_data,
            lab,
            accepts_offers,
            time_zone,
            open_hours,
            when_last_updated
        )
        SELECT venue_id,
            venue_external_id,
            region_id,
            market,
            name,
            location_lat,
            location_long,
            address_line1,
            address_line2,
            address_line3,
            post_code,
            venue_type_code,
            is_hidden,
            region,
            features,
            extended_data,
            lab,
            accepts_offers,
            time_zone,
            open_hours,
            when_last_updated 
        FROM temp_mcd_venues
        ON CONFLICT (venue_id) DO UPDATE SET
            venue_external_id=EXCLUDED.venue_external_id,
            region_id=EXCLUDED.region_id,
            market=EXCLUDED.market,
            name=EXCLUDED.name,
            location_lat=EXCLUDED.location_lat,
            location_long=EXCLUDED.location_long,
            address_line1=EXCLUDED.address_line1,
            address_line2=EXCLUDED.address_line2,
            address_line3=EXCLUDED.address_line3,
            post_code=EXCLUDED.post_code,
            venue_type_code=EXCLUDED.venue_type_code,
            is_hidden=EXCLUDED.is_hidden,
            region=EXCLUDED.region,
            features=EXCLUDED.features,
            extended_data=EXCLUDED.extended_data,
            lab=EXCLUDED.lab,
            accepts_offers=EXCLUDED.accepts_offers,
            time_zone=EXCLUDED.time_zone,
            open_hours=EXCLUDED.open_hours,
            when_last_updated=EXCLUDED.when_last_updated;
        """
        await conn.execute(text(upsert_query))

@task(retries=3,retry_delay_seconds=10,log_prints=True)
async def extract_and_load():
    client=AsyncIOMotorClient(mongo_uri)
    db=client[mongo_db]
    cursor=db["mcd_venues"].find().batch_size(100000)
    columns=[
        'venue_id', 
        'venue_external_id', 
        'region_id', 
        'market', 
        'name', 
        'location_lat', 
        'location_long', 
        'address_line1', 
        'address_line2', 
        'address_line3', 
        'post_code', 
        'venue_type_code', 
        'is_hidden', 
        'region', 
        'features', 
        'extended_data', 
        'lab', 
        'accepts_offers', 
        'time_zone', 
        'open_hours', 
        'when_last_updated'
    ]
    batch=[]
    chunk_size=1000000
    total=0
    async for doc in cursor:
        row=(
            to_int(doc.get("id")),
            doc.get("externalid"),
            doc.get("region_id"),
            doc.get("market"),
            doc.get("name"),
            doc.get("locationlat"),
            doc.get("locationlong"),
            doc.get("addressline1"),
            doc.get("addressline2"),
            doc.get("addressline3"),
            doc.get("postcode"),
            doc.get("venuetypecode"),
            to_bool(doc.get("ishidden")),
            doc.get("region"),
            doc.get("features"),
            doc.get("extendeddata"),
            to_bool(doc.get("lab")),
            to_bool(doc.get("acceptsoffers")),
            doc.get("timezone"),
            doc.get("openhours"),
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

@flow(name="venues-etl")
async def etl_flow():
    await extract_and_load()

if __name__=="__main__":
    asyncio.run(etl_flow())
