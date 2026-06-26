from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.consumer_activities_model import ConsumerActivities
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_consumer_activities(db: AsyncSession, mongo_doc: dict):
    stmt=insert(ConsumerActivities).values(
        activity_id=to_uuid(mongo_doc.get('activity_id')),
        action_type_code=to_int(mongo_doc.get('action_type_code')),
        action_type_name=mongo_doc.get('action_type_name'),
        action_type_detail=mongo_doc.get('action_type_detail'),
        device_type_code=mongo_doc.get('device_type_code'),
        device_type_name=mongo_doc.get('device_type_name'),
        market=mongo_doc.get('market'),
        reporting_id=to_uuid(mongo_doc.get('reporting_id')),
        date=to_datetime(mongo_doc.get('date')),
        activity_source_time_local=to_datetime(mongo_doc.get('activity_source_time_local')),
        activity_source_time_utc=to_datetime(mongo_doc.get('activity_source_time_utc')),
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "activity_id": to_uuid(mongo_doc.get('activity_id')),
            "action_type_code": to_int(mongo_doc.get('action_type_code')),
            "action_type_name": mongo_doc.get('action_type_name'),
            "action_type_detail": mongo_doc.get('action_type_detail'),
            "device_type_code": mongo_doc.get('device_type_code'),
            "device_type_name": mongo_doc.get('device_type_name'),
            "market": mongo_doc.get('market'),
            "reporting_id": to_uuid(mongo_doc.get('reporting_id')),
            "date": to_datetime(mongo_doc.get('date')),
            "activity_source_time_local": to_datetime(mongo_doc.get('activity_source_time_local')),
            "activity_source_time_utc": to_datetime(mongo_doc.get('activity_source_time_utc')),
        }
    )
    await db.execute(stmt)
    await db.commit()
