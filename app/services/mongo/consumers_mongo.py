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

    doc_external = mongo_conn["mcd_consumer_external_ids"].find_one({"reporting_id": reporting_id})
    external_id = None
    if doc_external:
        external_id = doc_external.get("external_id")

    new_values = dict(
        reporting_id=to_uuid(mongo_doc.get('reporting_id')),
        consumer_external_id=external_id,
        market=mongo_doc.get('market'),
        full_name=mongo_doc.get('full_name'),
        phone_number=mongo_doc.get('phone_number'),
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

        pipeline = [
            { "$match": current_query },
            { "$sort": { "_id": 1 }},
            { "$limit": batch_size },
            {
                "$lookup": {
                    "from": "mcd_consumer_external_ids",
                    "localField": "reportingid",
                    "foreignField": "reporting_id",
                    "as": "external_info"
                }
            },
            {
                "$unwind": {
                    "path": "$external_info",
                    "preserveNullAndEmptyArrays": True
                }
            }
        ]

        cursor = mongo_conn["mcd_consumer"].aggregate(pipeline)
        result = await cursor.to_list(length=batch_size)

        if not result:
            break

        data_list = [
            {
                "reporting_id": to_uuid(mongo_doc.get('reportingid')),
                "consumer_external_id": (mongo_doc.get('external_info') or {}).get('external_id'),
                "market": mongo_doc.get('market'),
                "full_name": mongo_doc.get('fullname'),
                "phone_number": mongo_doc.get('phone_number'),
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
        
        try:
            upsert = await _upsert_batch(
                db=db,
                model=DimConsumers,
                data_list=data_list,
                index_elements=["reporting_id"],
                exclude_from_update=["reporting_id"]
            )
            await db.commit()
        except Exception:
            await db.rollback()
            raise 

        last_id = result[-1]["_id"]

        if len(result) < batch_size:
            break
