from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.advertisements_model import Advertisements
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_advertisements(db: AsyncSession, mongo_doc: dict):
    stmt=insert(Advertisements).values(
        advertisement_id=to_int(mongo_doc.get('advertisement_id')),
        campaign_id=to_int(mongo_doc.get('campaign_id')),
        market=mongo_doc.get('market'),
        name=mongo_doc.get('name'),
        title=mongo_doc.get('title'),
        description=mongo_doc.get('description'),
        click_through_url=mongo_doc.get('click_through_url'),
        status=to_int(mongo_doc.get('status')),
        channel_code=mongo_doc.get('channel_code'),
        placement_code=mongo_doc.get('placement_code'),
        no_compete_group=mongo_doc.get('no_compete_group'),
        enable_time_based_weight=to_bool(mongo_doc.get('enable_time_based_weight')),
        enable_distance_weight=to_bool(mongo_doc.get('enable_distance_weight')),
        apply_geo_fence_filters=to_bool(mongo_doc.get('apply_geo_fence_filters')),
        apply_tag_value_filter=to_bool(mongo_doc.get('apply_tag_value_filter')),
        weight=to_int(mongo_doc.get('weight')),
        days_of_week=mongo_doc.get('days_of_week'),
        daily_start_time=to_int(mongo_doc.get('daily_start_time')),
        daily_end_time=to_int(mongo_doc.get('daily_end_time')),
        date_modified=to_datetime(mongo_doc.get('date_modified')),
        date_created=to_datetime(mongo_doc.get('date_created')),
        start_date=to_datetime(mongo_doc.get('start_date')),
        end_date=to_datetime(mongo_doc.get('end_date')),
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "advertisement_id": to_int(mongo_doc.get('advertisement_id')),
            "campaign_id": to_int(mongo_doc.get('campaign_id')),
            "market": mongo_doc.get('market'),
            "name": mongo_doc.get('name'),
            "title": mongo_doc.get('title'),
            "description": mongo_doc.get('description'),
            "click_through_url": mongo_doc.get('click_through_url'),
            "status": to_int(mongo_doc.get('status')),
            "channel_code": mongo_doc.get('channel_code'),
            "placement_code": mongo_doc.get('placement_code'),
            "no_compete_group": mongo_doc.get('no_compete_group'),
            "enable_time_based_weight": to_bool(mongo_doc.get('enable_time_based_weight')),
            "enable_distance_weight": to_bool(mongo_doc.get('enable_distance_weight')),
            "apply_geo_fence_filters": to_bool(mongo_doc.get('apply_geo_fence_filters')),
            "apply_tag_value_filter": to_bool(mongo_doc.get('apply_tag_value_filter')),
            "weight": to_int(mongo_doc.get('weight')),
            "days_of_week": mongo_doc.get('days_of_week'),
            "daily_start_time": to_int(mongo_doc.get('daily_start_time')),
            "daily_end_time": to_int(mongo_doc.get('daily_end_time')),
            "date_modified": to_datetime(mongo_doc.get('date_modified')),
            "date_created": to_datetime(mongo_doc.get('date_created')),
            "start_date": to_datetime(mongo_doc.get('start_date')),
            "end_date": to_datetime(mongo_doc.get('end_date')),
        }
    )
    await db.execute(stmt)
    await db.commit()
