from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.pushmessage_activities_model import PushmessageActivities
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_pushmessage_activities(db: AsyncSession, mongo_doc: dict):
    activity_id=to_uuid(mongo_doc.get('activity_id'))
    
    existing = await db.execute(
        select(PushmessageActivities).where(
            PushmessageActivities.activity_id == activity_id
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        activity_id=activity_id,
        activity_data=mongo_doc.get('activity_data'),
        action_type_code=to_int(mongo_doc.get('action_type_code')),
        action_type_name=mongo_doc.get('action_type_name'),
        device_type_code=mongo_doc.get('device_type_code'),
        device_type_name=mongo_doc.get('device_type_name'),
        message_id=to_int(mongo_doc.get('message_id')),
        message_name=mongo_doc.get('message_name'),
        num_messages=to_int(mongo_doc.get('num_messages')),
        market=mongo_doc.get('market'),
        reporting_id=to_uuid(mongo_doc.get('reporting_id')),
        date=to_datetime(mongo_doc.get('date')),
        activity_source_time_local=to_datetime(mongo_doc.get('activity_source_time_local')),
        activity_source_time_utc=to_datetime(mongo_doc.get('activity_source_time_utc'))
    )

    stmt=insert(PushmessageActivities).values(**new_values).on_conflict_do_update(
        index_elements=["activity_id"],
        set_=new_values
    )
    await db.execute(stmt)

    if existing_record is not None:
        StagingDataChangesService(
            db=db,
            module_name="mcd_pushmessage_activities",
            module_id=activity_id,
            old_data=existing_record,
            new_data=new_values
        )
        
    await db.commit()
