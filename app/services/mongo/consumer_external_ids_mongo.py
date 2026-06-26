from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.consumer_external_ids_model import ConsumerExternalIds
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_consumer_external_ids(db: AsyncSession, mongo_doc: dict):
    stmt=insert(ConsumerExternalIds).values(
        reporting_id=to_uuid(mongo_doc.get('reporting_id')),
        external_id=mongo_doc.get('external_id'),
        market=mongo_doc.get('market'),
        deleted_flag=to_bool(mongo_doc.get('deleted_flag')),
        timestamp=to_datetime(mongo_doc.get('timestamp')),
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "reporting_id": to_uuid(mongo_doc.get('reporting_id')),
            "external_id": mongo_doc.get('external_id'),
            "market": mongo_doc.get('market'),
            "deleted_flag": to_bool(mongo_doc.get('deleted_flag')),
            "timestamp": to_datetime(mongo_doc.get('timestamp')),
        }
    )
    await db.execute(stmt)
    await db.commit()
