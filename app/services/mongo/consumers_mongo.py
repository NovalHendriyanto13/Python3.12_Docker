from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from configs.mongo import mongo_conn
from app.models.dim_consumer_model import DimConsumers
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.helpers.db_helper import _upsert_batch
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_consumers(db: AsyncSession, mongo_doc: dict):
    reporting_id=to_uuid(mongo_doc.get('reporting_id'))
    
    existing = await db.execute(
        select(DimConsumers).where(
            DimConsumers.reporting_id == reporting_id
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        reporting_id=to_uuid(mongo_doc.get('reporting_id')),
        market=mongo_doc.get('market'),
        full_name=mongo_doc.get('full_name'),
        email_address=mongo_doc.get('email_address'),
        gender=mongo_doc.get('gender'),
        date_of_birth=to_datetime(mongo_doc.get('date_of_birth')),
        postcode=mongo_doc.get('postcode'),
        is_deactivated=to_bool(mongo_doc.get('is_deactivated')),
        registration_type=to_int(mongo_doc.get('registration_type')),
        consumer_type=to_int(mongo_doc.get('consumer_type')),
        registration_source=mongo_doc.get('registration_source'),
        creation_date=to_datetime(mongo_doc.get('creation_date')),
        modified_date=to_datetime(mongo_doc.get('modified_date')),
    )

    upsert = await _upsert_batch(
        db=db,
        model=DimConsumers,
        data_list=[new_values],
        index_elements=["reporting_id"],
        exclude_from_update=["reporting_id"]
    )

    if existing_record is not None:
        StagingDataChangesService(
            db=db,
            module_name="dim_consumers",
            module_id=reporting_id,
            old_data=existing_record,
            new_data=new_values
        )

    await db.commit()

async def consumers_init(
    db: AsyncSession
):
    batch_size = 1000
    last_id = None
    query = {}

    while True:
        current_query = dict(query)
        if last_id is not None:
            current_query["_id"] = { "$gt": last_id }

        cursor = mongo_conn["mcd_consumer"].find(current_query).sort("_id", 1).limit(batch_size)
        result = await cursor.to_list(length=batch_size)

        if not result:
            break

        data_list = [
            {
                "reporting_id": to_uuid(mongo_doc.get('reportingid')),
                "market": mongo_doc.get('market'),
                "full_name": mongo_doc.get('fullname'),
                "email_address": mongo_doc.get('emailaddress'),
                "gender": mongo_doc.get('gender'),
                "date_of_birth": to_datetime(mongo_doc.get('dateofbirth')),
                "postcode": mongo_doc.get('postcode'),
                "is_deactivated": to_bool(mongo_doc.get('isdeactivated')),
                "registration_type": to_int(mongo_doc.get('registrationtype')),
                "consumer_type": to_int(mongo_doc.get('consumertype')),
                "registration_source": mongo_doc.get('registrationsource'),
                "creation_date": to_datetime(mongo_doc.get('creationdate')),
                "modified_date": to_datetime(mongo_doc.get('modifieddate')),
            }
            for mongo_doc in result
        ]
        upsert = await _upsert_batch(
            db=db,
            model=DimConsumers,
            data_list=data_list,
            index_elements=["reporting_id"],
            exclude_from_update=["reporting_id"]
        )

        last_id = result[-1]["_id"]

        if len(result) < batch_size:
            break
