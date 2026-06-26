from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.stamp_program_transactions_model import StampProgramTransactions
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_stamp_program_transactions(db: AsyncSession, mongo_doc: dict):
    stmt=insert(StampProgramTransactions).values(
        activity_id=to_uuid(mongo_doc.get('activity_id')),
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
        activity_source_time_utc=to_datetime(mongo_doc.get('activity_source_time_utc')),
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "activity_id": to_uuid(mongo_doc.get('activity_id')),
            "activity_data": mongo_doc.get('activity_data'),
            "action_type_code": to_int(mongo_doc.get('action_type_code')),
            "action_type_name": mongo_doc.get('action_type_name'),
            "device_type_code": mongo_doc.get('device_type_code'),
            "device_type_name": mongo_doc.get('device_type_name'),
            "message_id": to_int(mongo_doc.get('message_id')),
            "message_name": mongo_doc.get('message_name'),
            "num_messages": to_int(mongo_doc.get('num_messages')),
            "market": mongo_doc.get('market'),
            "reporting_id": to_uuid(mongo_doc.get('reporting_id')),
            "date": to_datetime(mongo_doc.get('date')),
            "activity_source_time_local": to_datetime(mongo_doc.get('activity_source_time_local')),
            "activity_source_time_utc": to_datetime(mongo_doc.get('activity_source_time_utc')),
        }
    )
    await db.execute(stmt)
    await db.commit()
