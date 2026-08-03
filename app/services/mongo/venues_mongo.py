from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from configs.mongo import mongo_conn
from app.models.dim_venues_model import DimVenues
from app.helpers.app_helper import to_int, to_bool
from app.helpers.db_helper import _upsert_batch

async def upsert_venues(db: AsyncSession, mongo_doc: dict):
    venue_id = to_int(mongo_doc.get('venue_id'))

    new_values = dict(
        venue_key=venue_id,
        venue_external_id=mongo_doc.get('venue_external_id'),
        market=mongo_doc.get('market'),
        name=mongo_doc.get('name'),
        venue_type_code=mongo_doc.get('venue_type_code'),
        is_hidden=to_bool(mongo_doc.get('is_hidden')),
        region=mongo_doc.get('region'),
    )

    upsert = await _upsert_batch(
        db=db,
        model=DimVenues,
        data_list=[new_values],
        index_elements=["venue_id"],
        exclude_from_update=["venue_id"]
    )

    await db.commit()

async def venues_init(
    db: AsyncSession
):
    batch_size = 1000
    last_id = None
    query = {}

    while True:
        current_query = dict(query)
        if last_id is not None:
            current_query["_id"] = { "$gt": last_id }

        cursor = mongo_conn["mcd_venues"].find(current_query).sort("_id", 1).limit(batch_size)
        result = await cursor.to_list(length=batch_size)

        if not result:
            break

        data_list = [
            {
                "venue_key": to_int(mongo_doc.get('id')),
                "venue_external_id": mongo_doc.get('externalid'),
                "market": mongo_doc.get('market'),
                "name": mongo_doc.get('name'),
                "venue_type_code": mongo_doc.get('venue_type_code'),
                "is_hidden": to_bool(mongo_doc.get('is_hidden')),
                "region": mongo_doc.get('region'),
            }
            for mongo_doc in result
        ]
        try:
            upsert = await _upsert_batch(
                db=db,
                model=DimVenues,
                data_list=data_list,
                index_elements=["venue_key"],
                exclude_from_update=["venue_key"]
            )

            last_id = result[-1]["_id"]

            if len(result) < batch_size:
                break
            
            await db.commit()
            print(f"🚀 Bulk Sync Finished! the process successfully upserted.")
        except Exception:
            await db.rollback()
            raise
        
