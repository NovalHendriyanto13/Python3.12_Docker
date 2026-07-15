from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.dim_venues_model import DimVenues
from app.helpers.app_helper import to_int, to_bool

async def upsert_venues(db: AsyncSession, mongo_doc: dict):
    venue_id = to_int(mongo_doc.get('venue_id'))

    new_values = dict(
        venue_id=venue_id,
        venue_external_id=mongo_doc.get('venue_external_id'),
        market=mongo_doc.get('market'),
        name=mongo_doc.get('name'),
        venue_type_code=mongo_doc.get('venue_type_code'),
        is_hidden=to_bool(mongo_doc.get('is_hidden')),
        region=mongo_doc.get('region'),
    )

    stmt=insert(DimVenues).values(**new_values).on_conflict_do_update(
        index_elements=["venue_id"],
        set_=new_data
    )
    await db.execute(stmt)

    await db.commit()
