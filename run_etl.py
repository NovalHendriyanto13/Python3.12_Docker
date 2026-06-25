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
from etl.loyalty_stamp_card_reward_transactions_etl import etl_loyalty_stamp_card_reward_transactions_flow
from etl.sales_headers_etl import etl_sales_headers_flow
from etl.advertisement_activities_etl import etl_advertisement_activities_flow
from etl.bonus_points_breakdown_etl import etl_bonus_points_breakdown_flow
from etl.campaigns_etl import etl_campaigns_flow
# from etl.consumer_activities_etl import etl_consumer_activities_flow
from etl.customer_externalID_etl import etl_consumer_external_ids_flow
# from etl.consumer_tags_etl import etl_consumer_tags_flow
from etl.gdpr_consumer_consent_events_etl import etl_gdpr_consumer_consent_events_flow
from etl.gdpr_consumer_consent_snapshot_etl import etl_gdpr_consumer_consent_snapshot_flow
from etl.loyalty_activities_etl import etl_loyalty_activities_flow
from etl.messages_etl import etl_messages_flow
from etl.offer_activities_etl import etl_offer_activities_flow
from etl.sales_detail_etl import etl_sales_detail_flow
from etl.venues_etl import etl_venues_flow

@flow(name="master-etl", log_prints=True)
async def master_flow():
    print("🚀 Master ETL started...")

    # Jalankan semua PARALLEL sekaligus
    await asyncio.gather(
        # etl_consumer_flow(),
        # etl_offers_flow(),
        # etl_advertisement_flow(),
        # etl_loyalty_points_card_transactions_flow(), 
        # etl_loyalty_stamp_card_transactions_flow(),
        # etl_loyalty_stamp_card_reward_transactions_flow(),
        # etl_sales_headers_flow(),
        # etl_advertisement_activities_flow(),
        # etl_bonus_points_breakdown_flow(), // NO CSV DATA
        etl_campaigns_flow(),
        # etl_consumer_activities_flow(), // tinjau ulang
        etl_consumer_external_ids_flow(),
        # etl_consumer_tags_flow(), // NO CSV
        etl_gdpr_consumer_consent_events_flow(),
        etl_gdpr_consumer_consent_snapshot_flow(),
        etl_loyalty_activities_flow(),
        etl_messages_flow(),
        etl_offer_activities_flow(),
        etl_sales_detail_flow(),
        etl_venues_flow(),
    )

    print("✅ Master ETL finished!")

if __name__ == "__main__":
    asyncio.run(master_flow())