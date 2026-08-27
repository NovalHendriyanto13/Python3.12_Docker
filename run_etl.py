# run_etl.py
import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from prefect import flow
from configs.app_config import etl_source
from etl.initialization.sales_init import etl_sales_flow
from etl.initialization.consumers_init import etl_consumers_flow
from etl.initialization.venues_init import etl_venues_flow
from etl.initialization.visit_init import etl_visit_flow
from etl.initialization.app_activity_init import etl_app_activity_flow
from etl.initialization.products_init import etl_product_flow

@flow(name="master-etl", log_prints=True)
async def master_flow():
    print("🚀 Master ETL started...")

    # if etl_source == 'PDC':
    #     from etl.PDC.advertisement_activities_etl import etl_advertisement_activities_flow
    #     from etl.PDC.advertisements_etl import etl_advertisements_flow
    #     from etl.PDC.bonus_points_breakdown_etl import etl_bonus_points_breakdown_flow
    #     from etl.PDC.campaigns_etl import etl_campaigns_flow
    #     from etl.PDC.consumer_activities_etl import etl_consumer_activities_flow
    #     from etl.PDC.consumers_etl import etl_consumers_flow
    #     from etl.PDC.consumer_externalID_etl import etl_consumer_externalID_flow
    #     # from etl.PDC.customer_tags_etl import etl_customer_tags_flow
    #     from etl.PDC.gdpr_consumer_consent_events_etl import etl_gdpr_consumer_consent_events_flow
    #     from etl.PDC.gdpr_consumer_consent_snapshot_etl import etl_gdpr_consumer_consent_snapshot_flow
    #     from etl.PDC.loyalty_activities_etl import etl_loyalty_activities_flow
    #     from etl.PDC.loyalty_program_rewards_etl import etl_loyalty_program_rewards_flow
    #     from etl.PDC.loyalty_programs_etl import etl_loyalty_programs_flow
    #     from etl.PDC.messages_etl import etl_messages_flow
    #     from etl.PDC.offer_activities_etl import etl_offer_activities_flow
    #     from etl.PDC.offers_etl import etl_offers_flow
    #     from etl.PDC.points_program_transactions_etl import etl_points_program_transactions_flow
    #     from etl.PDC.pushmessage_activities_etl import etl_pushmessage_activities_flow
    #     from etl.PDC.sale_details_etl import etl_sale_details_flow
    #     from etl.PDC.sales_headers_etl import etl_sales_headers_flow
    #     from etl.PDC.stamp_program_reward_transactions_etl import etl_stamp_program_reward_transactions_flow
    #     from etl.PDC.stamp_program_transactions import etl_stamp_program_transactions_flow
    #     from etl.PDC.tag_values_etl import etl_tag_values_flow
    #     from etl.PDC.venues_etl import etl_venues_flow
    # elif etl_source == 'BDE':
    #     from etl.BDE.advertisement_activities_etl import etl_advertisement_activities_flow
    #     from etl.BDE.advertisements_etl import etl_advertisements_flow
    #     from etl.BDE.bonus_points_breakdown_etl import etl_bonus_points_breakdown_flow
    #     from etl.BDE.campaigns_etl import etl_campaigns_flow
    #     from etl.BDE.consumer_activities_etl import etl_consumer_activities_flow
    #     from etl.PDC.consumers_etl import etl_consumers_flow
    #     from etl.PDC.consumer_externalID_etl import etl_consumer_externalID_flow
    #     # from etl.BDE.customer_tags_etl import etl_customer_tags_flow
    #     from etl.BDE.gdpr_consumer_consent_events_etl import etl_gdpr_consumer_consent_events_flow
    #     from etl.BDE.gdpr_consumer_consent_snapshot_etl import etl_gdpr_consumer_consent_snapshot_flow
    #     from etl.BDE.loyalty_activities_etl import etl_loyalty_activities_flow
    #     from etl.BDE.loyalty_program_rewards_etl import etl_loyalty_program_rewards_flow
    #     from etl.BDE.loyalty_programs_etl import etl_loyalty_programs_flow
    #     from etl.BDE.messages_etl import etl_messages_flow
    #     from etl.BDE.offer_activities_etl import etl_offer_activities_flow
    #     from etl.BDE.offers_etl import etl_offers_flow
    #     from etl.BDE.points_program_transactions_etl import etl_points_program_transactions_flow
    #     from etl.BDE.pushmessage_activities_etl import etl_pushmessage_activities_flow
    #     from etl.BDE.sale_details_etl import etl_sale_details_flow
    #     from etl.BDE.sales_headers_etl import etl_sales_headers_flow
    #     from etl.BDE.stamp_program_reward_transactions_etl import etl_stamp_program_reward_transactions_flow
    #     from etl.BDE.stamp_program_transactions import etl_stamp_program_transactions_flow
    #     from etl.BDE.tag_values_etl import etl_tag_values_flow
    #     from etl.BDE.venues_etl import etl_venues_flow
    # else:
        # etl_advertisement_activities_flow = None
        # etl_advertisements_flow = None
        # etl_bonus_points_breakdown_flow = None
        # etl_campaigns_flow = None
        # etl_consumer_activities_flow = None
        # etl_consumers_flow = None
        # etl_consumer_externalID_flow = None
        # # etl_consumer_tags_flow = None
        # etl_gdpr_consumer_consent_events_flow = None
        # etl_gdpr_consumer_consent_snapshot_flow = None
        # etl_loyalty_activities_flow = None
        # etl_loyalty_program_rewards_flow = None
        # etl_loyalty_programs_flow = None
        # etl_messages_flow = None
        # etl_offer_activities_flow = None
        # etl_offers_flow = None
        # etl_points_program_transactions_flow = None
        # etl_pushmessage_activities_flow = None
        # etl_sale_details_flow = None
        # etl_sales_headers_flow = None
        # etl_stamp_program_reward_transactions_flow = None
        # etl_stamp_program_transactions_flow = None
        # etl_tag_values_flow = None
        # etl_venues_flow = None

    # Jalankan semua PARALLEL sekaligus
    await asyncio.gather(
        # etl_consumers_flow(),
        # etl_venues_flow(),
        # etl_sales_flow(),
        # etl_visit_flow(),
        etl_app_activity_flow(),
        # etl_product_flow(),

        # etl_advertisement_activities_flow(),
        # etl_advertisements_flow(),
        # etl_bonus_points_breakdown_flow(),
        # etl_campaigns_flow(),
        # etl_consumer_activities_flow(),
        # etl_consumers_flow(),
        # etl_consumer_externalID_flow(),
        # etl_consumer_tags_flow(),
        # etl_gdpr_consumer_consent_events_flow(),
        # etl_gdpr_consumer_consent_snapshot_flow(),
        # etl_loyalty_activities_flow(),
        # etl_loyalty_program_rewards_flow(),
        # etl_loyalty_programs_flow(),
        # etl_messages_flow(),
        # etl_offer_activities_flow(),
        # etl_offers_flow(),
        # etl_points_program_transactions_flow(),
        # etl_pushmessage_activities_flow(),
        # etl_sale_details_flow(),
        # etl_sales_headers_flow(),
        # etl_stamp_program_reward_transactions_flow(),
        # etl_stamp_program_transactions_flow(),
        # etl_tag_values_flow(),
        # etl_venues_flow(),
    )

    print("✅ Master ETL finished!")

if __name__ == "__main__":
    asyncio.run(master_flow())