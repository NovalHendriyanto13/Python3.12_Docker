import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from prefect import flow
# from app.tasks.sample_task import databricks_to_mongo_sync
from app.tasks.advertisement_activities_task import databricks_to_mongo_advertisement_activities_sync
from app.tasks.advertisements_task import databricks_to_mongo_advertisements_sync
from app.tasks.bonus_points_breakdown_task import databricks_to_mongo_bonus_points_breakdown_sync
from app.tasks.campaigns_task import databricks_to_mongo_campaigns_sync
from app.tasks.consumer_activities_task import databricks_to_mongo_consumer_activities_sync
from app.tasks.consumer_externalID_task import databricks_to_mongo_consumer_external_ids_sync
from app.tasks.consumers_task import databricks_to_mongo_consumers_sync
from app.tasks.consumer_tags_task import databricks_to_mongo_consumer_tags_sync
from app.tasks.gdpr_consumer_consent_events_task import databricks_to_mongo_gdpr_consumer_consent_events_sync
from app.tasks.gdpr_consumer_consent_snapshot_task import databricks_to_mongo_gdpr_consumer_consent_snapshot_sync
from app.tasks.loyalty_activities_task import databricks_to_mongo_loyalty_activities_sync
from app.tasks.loyalty_program_rewards_task import databricks_to_mongo_loyalty_program_rewards_sync
from app.tasks.loyalty_programs_task import databricks_to_mongo_loyalty_programs_sync
from app.tasks.messages_task import databricks_to_mongo_messages_sync
from app.tasks.offer_activities_task import databricks_to_mongo_offer_activities_sync
from app.tasks.offers_task import databricks_to_mongo_offers_sync
from app.tasks.points_program_transactions_task import databricks_to_mongo_points_program_transactions_sync
from app.tasks.pushmessage_activities_task import databricks_to_mongo_pushmessage_activities_sync
from app.tasks.sale_details_task import databricks_to_mongo_sale_details_sync
from app.tasks.sales_headers_task import databricks_to_mongo_sales_headers_sync
from app.tasks.stamp_program_reward_transactions_task import databricks_to_mongo_stamp_program_reward_transactions_sync
from app.tasks.stamp_program_transactions_task import databricks_to_mongo_stamp_program_transactions_sync
from app.tasks.tag_values_task import databricks_to_mongo_tag_values_sync
from app.tasks.venues_task import databricks_to_mongo_venues_sync
 
@flow(name="master-etl", log_prints=True)
async def master_flow():
    await asyncio.gather(
        # databricks_to_mongo_sync()
        databricks_to_mongo_advertisement_activities_sync(),
        databricks_to_mongo_advertisements_sync(),
        databricks_to_mongo_bonus_points_breakdown_sync(),
        databricks_to_mongo_campaigns_sync(),
        databricks_to_mongo_consumer_activities_sync(),
        databricks_to_mongo_consumer_external_ids_sync(),
        databricks_to_mongo_consumers_sync(),
        databricks_to_mongo_consumer_tags_sync(),
        databricks_to_mongo_gdpr_consumer_consent_events_sync(),
        databricks_to_mongo_gdpr_consumer_consent_snapshot_sync(),
        databricks_to_mongo_loyalty_activities_sync(),
        databricks_to_mongo_loyalty_program_rewards_sync(),
        databricks_to_mongo_loyalty_programs_sync(),
        databricks_to_mongo_messages_sync(),
        databricks_to_mongo_offer_activities_sync(),
        databricks_to_mongo_offers_sync(),
        databricks_to_mongo_points_program_transactions_sync(),
        databricks_to_mongo_pushmessage_activities_sync(),
        databricks_to_mongo_sale_details_sync(),
        databricks_to_mongo_sales_headers_sync(),
        databricks_to_mongo_stamp_program_reward_transactions_sync(),
        databricks_to_mongo_stamp_program_transactions_sync(),
        databricks_to_mongo_tag_values_sync(),
        databricks_to_mongo_venues_sync(),
    )

if __name__ == "__main__":
    asyncio.run(master_flow())