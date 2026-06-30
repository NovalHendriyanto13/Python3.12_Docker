from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.tag_values_model import TagValues
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_tag_values(db: AsyncSession, mongo_doc: dict):
    tag_value_id=to_uuid(mongo_doc.get('tag_value_id'))
    
    existing = await db.execute(
        select(TagValues).where(
            TagValues.tag_value_id == tag_value_id
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        tag_value_id=tag_value_id,
        name=mongo_doc.get('name'),
        reference_code=mongo_doc.get('reference_code'),
        consumer_visible=to_bool(mongo_doc.get('consumer_visible')),
        consumer_updateable=to_bool(mongo_doc.get('consumer_updateable')),
        market=mongo_doc.get('market')
    )

    stmt=insert(TagValues).values(**new_values).on_conflict_do_update(
        index_elements=["tag_value_id"],
        set_=new_values
    )
    await db.execute(stmt)

    if existing_record is not None:
        StagingDataChangesService(
            db=db,
            module_name="mcd_tag_values",
            module_id=tag_value_id,
            old_data=existing_record,
            new_data=new_values
        )
        
    await db.commit()
