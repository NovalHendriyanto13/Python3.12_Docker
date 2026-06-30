from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.venues_model import Venues
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_venues(db: AsyncSession, mongo_doc: dict):
    venue_id = to_int(mongo_doc.get('venue_id'))

    existing = await db.execute(
        select(Venus).where(
            Venus.venue_id == venue_id
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        venue_id=venue_id,
        venue_external_id=mongo_doc.get('venue_external_id'),
        region_id=to_int(mongo_doc.get('region_id')),
        market=mongo_doc.get('market'),
        name=mongo_doc.get('name'),
        location_lat=to_decimal(mongo_doc.get('location_lat')),
        location_long=to_decimal(mongo_doc.get('location_long')),
        address_line1=mongo_doc.get('address_line1'),
        address_line2=mongo_doc.get('address_line2'),
        address_line3=mongo_doc.get('address_line3'),
        post_code=mongo_doc.get('post_code'),
        venue_type_code=mongo_doc.get('venue_type_code'),
        is_hidden=to_bool(mongo_doc.get('is_hidden')),
        region=mongo_doc.get('region'),
        features=mongo_doc.get('features'),
        extended_data=mongo_doc.get('extended_data'),
        lab=to_bool(mongo_doc.get('lab')),
        accepts_offers=to_bool(mongo_doc.get('accepts_offers')),
        time_zone=mongo_doc.get('time_zone'),
        open_hours=mongo_doc.get('open_hours'),
        when_last_updated=to_datetime(mongo_doc.get('when_last_updated')),
    )

    stmt=insert(Venues).values(**new_values).on_conflict_do_update(
        index_elements=["venue_id"],
        set_=new_data
    )
    await db.execute(stmt)

    if existing_record is not None:
        StagingDataChangesService(
            db=db,
            module_name="mcd_venues",
            module_id=venue_id,
            old_data=existing_record,
            new_data=new_values
        )

    await db.commit()
