from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.gdpr_consumer_consent_snapshot_model import GdprConsumerConsentSnapshot
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_gdpr_consumer_consent_snapshot(db: AsyncSession, mongo_doc: dict):
    stmt=insert(GdprConsumerConsentSnapshot).values(
        reporting_id=to_uuid(mongo_doc.get('reporting_id')),
        market=mongo_doc.get('market'),
        consent_to_store_and_process=to_bool(mongo_doc.get('consent_to_store_and_process')),
        services=mongo_doc.get('services'),
        event_time_utc=to_datetime(mongo_doc.get('event_time_utc')),
        date=to_datetime(mongo_doc.get('date')),
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "reporting_id": to_uuid(mongo_doc.get('reporting_id')),
            "market": mongo_doc.get('market'),
            "consent_to_store_and_process": to_bool(mongo_doc.get('consent_to_store_and_process')),
            "services": mongo_doc.get('services'),
            "event_time_utc": to_datetime(mongo_doc.get('event_time_utc')),
            "date": to_datetime(mongo_doc.get('date')),
        }
    )
    await db.execute(stmt)
    await db.commit()
