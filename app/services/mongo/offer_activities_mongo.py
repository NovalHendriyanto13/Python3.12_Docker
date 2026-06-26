from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.offer_activities_model import OfferActivities
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_offer_activities(db: AsyncSession, mongo_doc: dict):
    stmt=insert(OfferActivities).values(
        activity_id=to_uuid(mongo_doc.get('activity_id')),
        activity_data=mongo_doc.get('activity_data'),
        action_type_code=to_int(mongo_doc.get('action_type_code')),
        action_type_name=mongo_doc.get('action_type_name'),
        device_type_code=mongo_doc.get('device_type_code'),
        device_type_name=mongo_doc.get('device_type_name'),
        offer_id=to_int(mongo_doc.get('offer_id')),
        offer_name=mongo_doc.get('offer_name'),
        venue_id=to_int(mongo_doc.get('venue_id')),
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
            "offer_id": to_int(mongo_doc.get('offer_id')),
            "offer_name": mongo_doc.get('offer_name'),
            "venue_id": to_int(mongo_doc.get('venue_id')),
            "market": mongo_doc.get('market'),
            "reporting_id": to_uuid(mongo_doc.get('reporting_id')),
            "date": to_datetime(mongo_doc.get('date')),
            "activity_source_time_local": to_datetime(mongo_doc.get('activity_source_time_local')),
            "activity_source_time_utc": to_datetime(mongo_doc.get('activity_source_time_utc')),
        }
    )
    await db.execute(stmt)
    await db.commit()
