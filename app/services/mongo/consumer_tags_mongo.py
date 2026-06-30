from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.consumer_tags_model import ConsumerTags
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_consumer_tags(db: AsyncSession, mongo_doc: dict):
    reporting_id=to_uuid(mongo_doc.get('reporting_id'))
    tag_value_id=to_uuid(mongo_doc.get('tag_value_id'))
    market=mongo_doc.get('market')
    
    existing = await db.execute(
        select(ConsumerTags).where(
            ConsumerTags.reporting_id == reporting_id,
            ConsumerTags.tag_value_id == tag_value_id,
            ConsumerTags.market == market
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        reporting_id=reporting_id,
        tag_value_id=tag_value_id,
        tag_assigned_time_utc=to_datetime(mongo_doc.get('tag_assigned_time_utc')),
        market=market,
        reference_code=mongo_doc.get('reference_code'),
    )

    stmt=insert(ConsumerTags).values(**new_values).on_conflict_do_update(
        index_elements=["reporting_id", "tag_value_id", "market"],
        set_=new_values
    )
    await db.execute(stmt)

    if existing_record is not None:
        StagingDataChangesService(
            db=db,
            module_name="mcd_consumer_tags",
            module_id=reporting_id + ":" + tag_value_id + ":" + market,
            old_data=existing_record,
            new_data=new_values
        )

    await db.commit()
