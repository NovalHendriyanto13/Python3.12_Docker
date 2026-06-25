import sys, os, asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool, to_uuid

TABLE_NAME="mcd_sale_details"
TARGET_TABLE="mcd_sale_details"

async def load_chunk_to_postgres(batch:list, columns:list):
    async with engine.begin() as conn:
        raw_conn=await conn.get_raw_connection()
        asyncpg_conn=raw_conn.driver_connection
        await conn.execute(text("CREATE TEMP TABLE temp_mcd_sale_details (LIKE mcd_sale_details EXCLUDING ALL) ON COMMIT DROP"))
        await asyncpg_conn.copy_records_to_table('temp_mcd_sale_details', records=batch, columns=columns)
        upsert_query="""
        INSERT INTO mcd_sale_details (
            sale_id,
            reporting_id,
            pos_transaction_id,
            offer_id,
            external_venue_id,
            market,
            product_code,
            quantity,
            unit_price,
            net_unit_price,
            tax_unit_amount,
            line_item_number,
            date,
            transaction_source_time_local
        )
        SELECT 
            sale_id,
            reporting_id,
            pos_transaction_id,
            offer_id,
            external_venue_id,
            market,
            product_code,
            quantity,
            unit_price,
            net_unit_price,
            tax_unit_amount,
            line_item_number,
            date,
            transaction_source_time_local 
        FROM temp_sale_details
        ON CONFLICT (sale_id,offer_id,product_code) DO UPDATE SET
            reporting_id=EXCLUDED.reporting_id,
            pos_transaction_id=EXCLUDED.pos_transaction_id,
            offer_id=EXCLUDED.offer_id,
            external_venue_id=EXCLUDED.external_venue_id,
            market=EXCLUDED.market,
            product_code=EXCLUDED.product_code,
            quantity=EXCLUDED.quantity,
            unit_price=EXCLUDED.unit_price,
            net_unit_price=EXCLUDED.net_unit_price,
            tax_unit_amount=EXCLUDED.tax_unit_amount,
            line_item_number=EXCLUDED.line_item_number,
            date=EXCLUDED.date,
            transaction_source_time_local=EXCLUDED.transaction_source_time_local;
        """
        await conn.execute(text(upsert_query))

@task(retries=3,retry_delay_seconds=10,log_prints=True)
async def extract_and_load():
    client=AsyncIOMotorClient(mongo_uri)
    db=client[mongo_db]
    cursor=db["mcd_sale_details"].find().batch_size(100000)
    columns=[
        'sale_id', 
        'reporting_id', 
        'pos_transaction_id', 
        'offer_id', 
        'external_venue_id', 
        'market', 
        'product_code', 
        'quantity', 
        'unit_price', 
        'net_unit_price', 
        'tax_unit_amount', 
        'line_item_number', 
        'date', 
        'transaction_source_time_local'
    ]
    batch=[]
    chunk_size=1000000
    total=0
    async for doc in cursor:
        row=(
            doc.get("saleid"),
            doc.get("reportingid"),
            doc.get("postransactionid"),
            to_int(doc.get("offerid")),
            doc.get("externalvenueid"),
            doc.get("market"),
            doc.get("productcode"),
            to_int(doc.get("quantity")),
            doc.get("unitprice"),
            doc.get("netunitprice"),
            doc.get("taxunitamount"),
            doc.get("lineitemnumber"),
            to_datetime(doc.get("date")),
            to_datetime(doc.get("transactionsourcetimelocal"))
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

@flow(name="sale_details-etl")
async def etl_flow():
    await extract_and_load()

if __name__=="__main__":
    asyncio.run(etl_flow())
