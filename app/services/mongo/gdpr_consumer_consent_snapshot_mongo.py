from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.gdpr_consumer_consent_snapshot_model import GdprConsumerConsentSnapshot
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_gdpr_consumer_consent_snapshot(db: AsyncSession, mongo_doc: dict):
    reporting_id = to_uuid(mongo_doc.get('reporting_id'))
    market = mongo_doc.get('market')
    services = mongo_doc.get('services')
    
    existing = await db.execute(
        select(GdprConsumerConsentSnapshot).where(
            GdprConsumerConsentSnapshot.reporting_id == reporting_id,
            GdprConsumerConsentSnapshot.market == market,
            GdprConsumerConsentSnapshot.services == services
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        reporting_id=reporting_id,
        market=market,
        consent_to_store_and_process=to_bool(mongo_doc.get('consent_to_store_and_process')),
        services=services,
        event_time_utc=to_datetime(mongo_doc.get('event_time_utc')),
        date=to_datetime(mongo_doc.get('date'))
    )

    stmt=insert(GdprConsumerConsentSnapshot).values(**new_values).on_conflict_do_update(
        index_elements=["reporting_id", "market", "services"],
        set_=new_values
    )
    await db.execute(stmt)

    if existing_record is not None:
        StagingDataChangesService(
            db = db,
            module_name = "mcd_gdpr_consumer_consent_snapshot",
            module_id = (reporting_id + ":" + market + ":" + services),
            old_data = existing_record,
            new_data = new_values
        )
        
    await db.commit()
