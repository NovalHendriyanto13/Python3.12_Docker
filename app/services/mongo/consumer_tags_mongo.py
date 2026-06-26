from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.consumer_tags_model import ConsumerTags
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_consumer_tags(db: AsyncSession, mongo_doc: dict):
    stmt=insert(ConsumerTags).values(
        reporting_id=to_uuid(mongo_doc.get('reporting_id')),
        tag_value_id=to_uuid(mongo_doc.get('tag_value_id')),
        tag_assigned_time_utc=to_datetime(mongo_doc.get('tag_assigned_time_utc')),
        market=mongo_doc.get('market'),
        reference_code=mongo_doc.get('reference_code'),
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "reporting_id": to_uuid(mongo_doc.get('reporting_id')),
            "tag_value_id": to_uuid(mongo_doc.get('tag_value_id')),
            "tag_assigned_time_utc": to_datetime(mongo_doc.get('tag_assigned_time_utc')),
            "market": mongo_doc.get('market'),
            "reference_code": mongo_doc.get('reference_code'),
        }
    )
    await db.execute(stmt)
    await db.commit()
