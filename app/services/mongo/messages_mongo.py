from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.messages_model import Messages
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_messages(db: AsyncSession, mongo_doc: dict):
    message_id=to_int(mongo_doc.get('message_id'))

    existing = await db.execute(
        select(Messages).where(
            Messages.message_id == message_id
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        message_id=message_id,
        market=mongo_doc.get('market'),
        status=to_int(mongo_doc.get('status')),
        name=mongo_doc.get('name'),
        subject=mongo_doc.get('subject'),
        body=mongo_doc.get('body'),
        email_text_body=mongo_doc.get('email_text_body'),
        offer_id=to_int(mongo_doc.get('offer_id')),
        recurring_type_code=to_int(mongo_doc.get('recurring_type_code')),
        daily_start_time=to_int(mongo_doc.get('daily_start_time')),
        daily_end_time=to_int(mongo_doc.get('daily_end_time')),
        time_frame_type=to_int(mongo_doc.get('time_frame_type')),
        date_modified=to_datetime(mongo_doc.get('date_modified')),
        date_created=to_datetime(mongo_doc.get('date_created')),
        time_frame_start_date=to_datetime(mongo_doc.get('time_frame_start_date')),
        time_frame_end_date=to_datetime(mongo_doc.get('time_frame_end_date'))
    )

    stmt=insert(Messages).values(**new_values).on_conflict_do_update(
        index_elements=["message_id"],
        set_=new_values
    )
    await db.execute(stmt)

    if existing_record is not None:
        StagingDataChangesService(
            db=db,
            module_name="mcd_messages",
            module_id=message_id,
            old_data=existing_record,
            new_data=new_values
        )
        
    await db.commit()
