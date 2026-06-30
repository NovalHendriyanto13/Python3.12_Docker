from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.consumer_external_ids_model import ConsumerExternalIds
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_consumer_external_ids(db: AsyncSession, mongo_doc: dict):
    reporting_id=to_uuid(mongo_doc.get('reporting_id'))
    external_id=mongo_doc.get('external_id')
    
    existing = await db.execute(
        select(ConsumerExternalIds).where(
            ConsumerExternalIds.reporting_id == reporting_id,
            ConsumerExternalIds.reporting_id == reporting_id
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        reporting_id=to_uuid(mongo_doc.get('reporting_id')),
        external_id=mongo_doc.get('external_id'),
        market=mongo_doc.get('market'),
        deleted_flag=to_bool(mongo_doc.get('deleted_flag')),
        timestamp=to_datetime(mongo_doc.get('timestamp'))
    )

    stmt=insert(ConsumerExternalIds).values(**new_values).on_conflict_do_update(
        index_elements=["reporting_id", "external_id"],
        set_=new_values
    )
    await db.execute(stmt)

    if existing_record is not None:
        StagingDataChangesService(
            db=db,
            module_name="mcd_consumer_external_ids",
            module_id=reporting_id + ":" + external_id,
            old_data=existing_record,
            new_data=new_values
        )

    await db.commit()
