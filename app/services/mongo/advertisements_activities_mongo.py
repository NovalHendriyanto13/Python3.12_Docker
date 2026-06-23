from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.advertisements_model import Advertisements
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_advertisements(db: AsyncSession, mongo_doc: dict):
    stmt = insert(Advertisements).values(

        advertisement_id = to_int(mongo_doc["id"]),  
        campaign_id = to_int(mongo_doc["campaignid"]),
        market = mongo_doc["market"],
        name = mongo_doc["name"],
        title = mongo_doc["title"],
        description = mongo_doc["description"],
        click_through_url = mongo_doc["click_through_url"],
        status = to_int(mongo_doc["status"]),
        channel_code = mongo_doc["channel_code"],
        placement_code = mongo_doc["placement_code"],
        no_compete_group = mongo_doc["no_compete_group"],
        enable_time_based_weight = to_bool(mongo_doc["enable_time_based_weight"]),
        enable_distance_weight = to_bool(mongo_doc["enable_distance_weight"]),
        apply_geo_fence_filters = to_bool(mongo_doc["apply_geo_fence_filters"]),
        apply_tag_value_filter = to_bool(mongo_doc["apply_tag_value_filter"]),
        weight = to_int(mongo_doc["weight"]),
        days_of_week = mongo_doc["days_of_week"],
        daily_start_time = to_int(mongo_doc["daily_start_time"]),
        daily_end_time = to_int(mongo_doc["daily_end_time"]),
        date_modified = to_datetime(mongo_doc["date_modified"]),
        date_created = to_datetime(mongo_doc["date_created"]),
        start_date = to_datetime(mongo_doc["startdate"]),
        end_date = to_datetime(mongo_doc["enddate"]),

    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "advertisement_id": to_int(mongo_doc["id"]),  
            "campaign_id": to_int(mongo_doc["campaignid"]),
            "market": mongo_doc["market"],
            "name": mongo_doc["name"],
            "title": mongo_doc["title"],
            "description": mongo_doc["description"],
            "click_through_url": mongo_doc["click_through_url"],
            "status": to_int(mongo_doc["status"]),
            "channel_code": mongo_doc["channel_code"],
            "placement_code": mongo_doc["placement_code"],
            "no_compete_group": mongo_doc["no_compete_group"],
            "enable_time_based_weight": to_bool(mongo_doc["enable_time_based_weight"]),
            "enable_distance_weight": to_bool(mongo_doc["enable_distance_weight"]),
            "apply_geo_fence_filters": to_bool(mongo_doc["apply_geo_fence_filters"]),
            "apply_tag_value_filter": to_bool(mongo_doc["apply_tag_value_filter"]),
            "weight": to_int(mongo_doc["weight"]),
            "days_of_week": mongo_doc["days_of_week"],
            "daily_start_time": to_int(mongo_doc["daily_start_time"]),
            "daily_end_time": to_int(mongo_doc["daily_end_time"]),
            "date_modified": to_datetime(mongo_doc["date_modified"]),
            "date_created": to_datetime(mongo_doc["date_created"]),
            "start_date": to_datetime(mongo_doc["startdate"]),
            "end_date": to_datetime(mongo_doc["enddate"]),
        }
    )
    await db.execute(stmt)
    await db.commit()
