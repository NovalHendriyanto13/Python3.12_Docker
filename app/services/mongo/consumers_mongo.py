from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.consumers_model import Consumers
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_consumers(db: AsyncSession, mongo_doc: dict):
    reporting_id=to_uuid(mongo_doc.get('reporting_id'))
    
    existing = await db.execute(
        select(Consumers).where(
            Consumers.reporting_id == reporting_id
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        reporting_id=to_uuid(mongo_doc.get('reporting_id')),
        last_known_device_id=mongo_doc.get('last_known_device_id'),
        market=mongo_doc.get('market'),
        first_name=mongo_doc.get('first_name'),
        last_name=mongo_doc.get('last_name'),
        full_name=mongo_doc.get('full_name'),
        email_address=mongo_doc.get('email_address'),
        gender=mongo_doc.get('gender'),
        date_of_birth=to_datetime(mongo_doc.get('date_of_birth')),
        phone_number=mongo_doc.get('phone_number'),
        postcode=mongo_doc.get('postcode'),
        is_deactivated=to_bool(mongo_doc.get('is_deactivated')),
        registration_type=to_int(mongo_doc.get('registration_type')),
        consumer_type=to_int(mongo_doc.get('consumer_type')),
        registration_source=mongo_doc.get('registration_source'),
        creation_date=to_datetime(mongo_doc.get('creation_date')),
        modified_date=to_datetime(mongo_doc.get('modified_date')),
        deactivation_date=to_datetime(mongo_doc.get('deactivation_date'))
    )

    stmt=insert(Consumers).values(**new_values).on_conflict_do_update(
        index_elements=["reporting_id"],
        set_=new_values
    )
    await db.execute(stmt)

    if existing_record is not None:
        StagingDataChangesService(
            db=db,
            module_name="mcd_consumers",
            module_id=reporting_id,
            old_data=existing_record,
            new_data=new_values
        )

    await db.commit()
