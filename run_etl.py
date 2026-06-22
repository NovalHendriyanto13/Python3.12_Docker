# run_etl.py
import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from prefect import flow
from etl.offers_etl import etl_offers_flow
from etl.consumers_etl import etl_consumer_flow
from etl.advertisements_etl import etl_advertisement_flow
from etl.loyalty_points_card_transactions_etl import etl_loyalty_points_card_transactions_flow
from etl.loyalty_stamp_card_transactions_etl import etl_loyalty_stamp_card_transactions_flow

@flow(name="master-etl", log_prints=True)
async def master_flow():
    print("🚀 Master ETL started...")

    # Jalankan semua PARALLEL sekaligus
    await asyncio.gather(
        # etl_consumer_flow(),
        # etl_offers_flow(),
        # etl_advertisement_flow(),
        # etl_loyalty_points_card_transactions_flow(), 
        etl_loyalty_stamp_card_transactions_flow() 
    )

    print("✅ Master ETL finished!")

if __name__ == "__main__":
    asyncio.run(master_flow())