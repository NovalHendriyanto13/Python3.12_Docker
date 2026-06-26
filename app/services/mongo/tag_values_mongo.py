from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.tag_values_model import TagValues
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_tag_values(db: AsyncSession, mongo_doc: dict):
    stmt=insert(TagValues).values(
        tag_value_id=to_uuid(mongo_doc.get('tag_value_id')),
        name=mongo_doc.get('name'),
        reference_code=mongo_doc.get('reference_code'),
        consumer_visible=to_bool(mongo_doc.get('consumer_visible')),
        consumer_updateable=to_bool(mongo_doc.get('consumer_updateable')),
        market=mongo_doc.get('market'),
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "tag_value_id": to_uuid(mongo_doc.get('tag_value_id')),
            "name": mongo_doc.get('name'),
            "reference_code": mongo_doc.get('reference_code'),
            "consumer_visible": to_bool(mongo_doc.get('consumer_visible')),
            "consumer_updateable": to_bool(mongo_doc.get('consumer_updateable')),
            "market": mongo_doc.get('market'),
        }
    )
    await db.execute(stmt)
    await db.commit()
