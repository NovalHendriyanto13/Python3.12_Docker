from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.messages_model import Messages
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_messages(db: AsyncSession, mongo_doc: dict):
    stmt=insert(Messages).values(
        message_id=to_int(mongo_doc.get('message_id')),
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
        time_frame_end_date=to_datetime(mongo_doc.get('time_frame_end_date')),
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "message_id": to_int(mongo_doc.get('message_id')),
            "market": mongo_doc.get('market'),
            "status": to_int(mongo_doc.get('status')),
            "name": mongo_doc.get('name'),
            "subject": mongo_doc.get('subject'),
            "body": mongo_doc.get('body'),
            "email_text_body": mongo_doc.get('email_text_body'),
            "offer_id": to_int(mongo_doc.get('offer_id')),
            "recurring_type_code": to_int(mongo_doc.get('recurring_type_code')),
            "daily_start_time": to_int(mongo_doc.get('daily_start_time')),
            "daily_end_time": to_int(mongo_doc.get('daily_end_time')),
            "time_frame_type": to_int(mongo_doc.get('time_frame_type')),
            "date_modified": to_datetime(mongo_doc.get('date_modified')),
            "date_created": to_datetime(mongo_doc.get('date_created')),
            "time_frame_start_date": to_datetime(mongo_doc.get('time_frame_start_date')),
            "time_frame_end_date": to_datetime(mongo_doc.get('time_frame_end_date')),
        }
    )
    await db.execute(stmt)
    await db.commit()
