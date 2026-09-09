import sys, os, asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool, to_uuid, to_decimal, to_deterministic_uuid

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
        FROM temp_mcd_sale_details
        ON CONFLICT (pos_transaction_id, sale_id, reporting_id, product_code) DO UPDATE SET
            offer_id=EXCLUDED.offer_id,
            external_venue_id=EXCLUDED.external_venue_id,
            market=EXCLUDED.market,
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

    # mcd_sale_details docs have no reporting_id field at all (it's part of this
    # table's composite PK, so it can't be left null) - but reporting_id is
    # available on the matching mcd_sale_headers doc via pos_transaction_id.
    # Pre-fetch that mapping once (7.6k headers) instead of a per-row query.
    reporting_id_by_pos_transaction_id = {}
    async for header in db["mcd_sale_headers"].find({}, {"postransactionid": 1, "reportingid": 1}):
        pos_id = header.get("postransactionid")
        if pos_id:
            reporting_id_by_pos_transaction_id[pos_id] = header.get("reportingid")

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
    chunk_size=1000000
    total=0
    # Real docs only ever carry: saleid, postransactionid, offerid, productcode,
    # quantity, unitprice, netunitprice, taxunitamount, dateoccurred. No
    # externalvenueid/market/lineitemnumber/transactionsourcetimelocal exist.
    #
    # (sale_id, pos_transaction_id, product_code) isn't unique in the source -
    # the same product can appear on more than one line within a sale - so rows
    # sharing that key are merged here (quantity summed) before upserting,
    # since the table's PK is exactly that 4-column combo (+ reporting_id) and
    # ON CONFLICT can't affect the same row twice within one statement.
    merged = {}
    async for doc in cursor:
        key = (doc.get("saleid"), doc.get("postransactionid"), doc.get("productcode"))
        if key in merged:
            merged[key]["quantity"] = (merged[key]["quantity"] or 0) + (to_int(doc.get("quantity")) or 0)
            continue
        pos_transaction_id = doc.get("postransactionid")
        merged[key] = {
            "sale_id": to_deterministic_uuid(doc.get("saleid")),
            "reporting_id": to_uuid(reporting_id_by_pos_transaction_id.get(pos_transaction_id)),
            "pos_transaction_id": to_uuid(pos_transaction_id),
            "offer_id": to_int(doc.get("offerid")),
            "product_code": doc.get("productcode"),
            "quantity": to_int(doc.get("quantity")) or 0,
            "unit_price": to_decimal(doc.get("unitprice")),
            "net_unit_price": to_decimal(doc.get("netunitprice")),
            "tax_unit_amount": to_decimal(doc.get("taxunitamount")),
            "date": to_datetime(doc.get("dateoccurred")),
        }

    batch = [
        (
            r["sale_id"], r["reporting_id"], r["pos_transaction_id"], r["offer_id"],
            None, None, r["product_code"], r["quantity"], r["unit_price"],
            r["net_unit_price"], r["tax_unit_amount"], None, r["date"], None,
        )
        for r in merged.values()
    ]
    for i in range(0, len(batch), chunk_size):
        chunk = batch[i:i + chunk_size]
        await load_chunk_to_postgres(chunk, columns)
        total += len(chunk)
        await asyncio.sleep(0.1)
    print(f"Finished sync {total} rows")

@flow(name="sale_details-etl")
async def etl_flow():
    await extract_and_load()

if __name__=="__main__":
    asyncio.run(etl_flow())
