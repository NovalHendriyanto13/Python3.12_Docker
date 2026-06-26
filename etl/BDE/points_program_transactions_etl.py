import sys, os, asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prefect import flow, task
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from configs.database import engine
from configs.app_config import mongo_uri, mongo_db
from app.helpers.app_helper import to_datetime, to_int, to_bool, to_uuid

TABLE_NAME="points_program_transactions"
TARGET_TABLE="points_program_transactions"

async def load_chunk_to_postgres(batch:list, columns:list):
    async with engine.begin() as conn:
        raw_conn=await conn.get_raw_connection()
        asyncpg_conn=raw_conn.driver_connection
        await conn.execute(text("CREATE TEMP TABLE temp_points_program_transactions (LIKE points_program_transactions EXCLUDING ALL) ON COMMIT DROP"))
        await asyncpg_conn.copy_records_to_table('temp_points_program_transactions', records=batch, columns=columns)
        upsert_query="""
        INSERT INTO points_program_transactions (points_program_transaction_id,points_program_transaction_group_id,pos_sales_transaction_id,sale_id,incentive_program_id,incentive_program_name,transaction_type,transaction_sub_type,transaction_origin,note,points_balance_after_transaction,points_delta,pointsrequested,external_transaction_reference,referenced_transactions,reward_type,reward_reference_id,venue_id,venue_name,venue_external_id,market,reporting_id,date,transaction_source_time_local,transaction_time_utc)
        SELECT points_program_transaction_id,points_program_transaction_group_id,pos_sales_transaction_id,sale_id,incentive_program_id,incentive_program_name,transaction_type,transaction_sub_type,transaction_origin,note,points_balance_after_transaction,points_delta,pointsrequested,external_transaction_reference,referenced_transactions,reward_type,reward_reference_id,venue_id,venue_name,venue_external_id,market,reporting_id,date,transaction_source_time_local,transaction_time_utc FROM temp_points_program_transactions
        ON CONFLICT (points_program_transaction_id) DO UPDATE SET
        points_program_transaction_group_id=EXCLUDED.points_program_transaction_group_id,pos_sales_transaction_id=EXCLUDED.pos_sales_transaction_id,sale_id=EXCLUDED.sale_id,incentive_program_id=EXCLUDED.incentive_program_id,incentive_program_name=EXCLUDED.incentive_program_name,transaction_type=EXCLUDED.transaction_type,transaction_sub_type=EXCLUDED.transaction_sub_type,transaction_origin=EXCLUDED.transaction_origin,note=EXCLUDED.note,points_balance_after_transaction=EXCLUDED.points_balance_after_transaction,points_delta=EXCLUDED.points_delta,pointsrequested=EXCLUDED.pointsrequested,external_transaction_reference=EXCLUDED.external_transaction_reference,referenced_transactions=EXCLUDED.referenced_transactions,reward_type=EXCLUDED.reward_type,reward_reference_id=EXCLUDED.reward_reference_id,venue_id=EXCLUDED.venue_id,venue_name=EXCLUDED.venue_name,venue_external_id=EXCLUDED.venue_external_id,market=EXCLUDED.market,reporting_id=EXCLUDED.reporting_id,date=EXCLUDED.date,transaction_source_time_local=EXCLUDED.transaction_source_time_local,transaction_time_utc=EXCLUDED.transaction_time_utc;
        """
        await conn.execute(text(upsert_query))

@task(retries=3,retry_delay_seconds=10,log_prints=True)
async def extract_and_load():
    client=AsyncIOMotorClient(mongo_uri)
    db=client[mongo_db]
    cursor=db["points_program_transactions"].find().batch_size(100000)
    columns=['points_program_transaction_id', 'points_program_transaction_group_id', 'pos_sales_transaction_id', 'sale_id', 'incentive_program_id', 'incentive_program_name', 'transaction_type', 'transaction_sub_type', 'transaction_origin', 'note', 'points_balance_after_transaction', 'points_delta', 'pointsrequested', 'external_transaction_reference', 'referenced_transactions', 'reward_type', 'reward_reference_id', 'venue_id', 'venue_name', 'venue_external_id', 'market', 'reporting_id', 'date', 'transaction_source_time_local', 'transaction_time_utc']
    batch=[]
    chunk_size=1000000
    total=0
    async for doc in cursor:
        row=(
            doc.get("pointsprogramtransactionid"),doc.get("pointsprogramtransactiongroupid"),doc.get("possalestransactionid"),doc.get("saleid"),to_int(doc.get("incentiveprogramid")),doc.get("incentiveprogramname"),doc.get("transactiontype"),doc.get("transactionsubtype"),doc.get("transactionorigin"),doc.get("note"),to_int(doc.get("pointsbalanceaftertransaction")),to_int(doc.get("pointsdelta")),to_int(doc.get("pointsrequested")),doc.get("externaltransactionreference"),to_int(doc.get("referencedtransactions")),doc.get("rewardtype"),doc.get("rewardreferenceid"),to_int(doc.get("venueid")),doc.get("venuename"),doc.get("venueexternalid"),doc.get("market"),doc.get("reportingid"),to_datetime(doc.get("date")),to_datetime(doc.get("transactionsourcetimelocal")),to_datetime(doc.get("transactiontimeutc"))
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

@flow(name="points_program_transactions-etl")
async def etl_flow():
    await extract_and_load()

if __name__=="__main__":
    asyncio.run(etl_flow())
