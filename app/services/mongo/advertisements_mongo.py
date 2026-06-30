from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.advertisements_model import Advertisements
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_advertisements(db: AsyncSession, mongo_doc: dict):
    advertisement_id=to_int(mongo_doc.get('advertisement_id')),
    
    existing = await db.execute(
        select(Advertisements).where(
            Advertisements.advertisement_id == advertisement_id
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        advertisement_id=advertisement_id,
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
        end_date=to_datetime(mongo_doc.get('end_date'))
    )

    stmt=insert(Advertisements).values(**new_values).on_conflict_do_update(
        index_elements=["advertisement_id"],
        set_=new_values
    )
    await db.execute(stmt)

    if existing_record is not None:
        StagingDataChangesService(
            db=db,
            module_name="mcd_advertisements",
            module_id=advertisement_id,
            old_data=existing_record,
            new_data=new_values
        )

    await db.commit()
