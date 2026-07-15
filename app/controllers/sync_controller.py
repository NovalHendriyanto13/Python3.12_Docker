from app.payloads.sync_requests.sync_request import SyncRequest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from configs.mongo import mongo_conn
from app.models.dim_consumer_model import DimConsumers
from app.models.dim_venues_model import DimVenues
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

class SyncController:
    @staticmethod
    async def sync_consumers_mongo(payload: SyncRequest, db: AsyncSession):
        collection = "mcd_consumer"
        batch_size = payload.per_batch
        total_synced = 0
        all_results = []

        cursor = mongo_conn[collection].find({})

        batch = []
        async for data in cursor:
            batch.append(dict(
                reporting_id=to_uuid(data.get('reporting_id')),
                market=data.get('market'),
                full_name=data.get('full_name'),
                email_address=data.get('email_address'),
                gender=data.get('gender'),
                date_of_birth=to_datetime(data.get('date_of_birth')),
                postcode=data.get('postcode'),
                is_deactivated=to_bool(data.get('is_deactivated')),
                registration_type=to_int(data.get('registration_type')),
                consumer_type=to_int(data.get('consumer_type')),
                registration_source=data.get('registration_source'),
                creation_date=to_datetime(data.get('creation_date')),
                modified_date=to_datetime(data.get('modified_date')),
            ))

            if len(batch) >= batch_size:
                synced = await SyncController._upsert_batch(
                    db=db,
                    data_list=batch,
                    model=DimConsumers,
                    index_elements=['reporting_id']
                )
                total_synced += synced
                batch = []

        if batch:
            synced = await SyncController._upsert_batch(
                db=db,
                data_list=batch,
                model=DimConsumers,
                index_elements=['reporting_id']
            )
            total_synced += synced

        return {"total_synced": total_synced}

    @staticmethod
    async def sync_venues_mongo(payload: SyncRequest, db: AsyncSession):
        collection = "mcd_venue"
        batch_size = payload.per_batch
        total_synced = 0
        all_results = []

        cursor = mongo_conn[collection].find({})

        batch = []
        async for data in cursor:
            batch.append(dict(
                venue_id=to_int(mongo_doc.get('venue_id')),
                venue_external_id=mongo_doc.get('venue_external_id'),
                market=mongo_doc.get('market'),
                name=mongo_doc.get('name'),
                venue_type_code=mongo_doc.get('venue_type_code'),
                is_hidden=to_bool(mongo_doc.get('is_hidden')),
                region=mongo_doc.get('region'),
            ))

            if len(batch) >= batch_size:
                synced = await SyncController._upsert_batch(
                    db=db,
                    data_list=batch,
                    model=DimVenues,
                    index_elements=['venue_id']
                )
                total_synced += synced
                batch = []

        if batch:
            synced = await SyncController._upsert_batch(
                db=db,
                data_list=batch,
                model=DimVenues,
                index_elements=['venue_id']
            )
            total_synced += synced

        return {"total_synced": total_synced}

    @staticmethod
    async def _upsert_batch(
        db: AsyncSession, 
        data_list: list[dict],
        model,
        index_elements: list[str],
        exclude_from_update: list[str] = None
    ) -> int:

        if not data_list:
            return 0

        if exclude_from_update is None:
            exclude_from_update = []

        excluded_cols = set(index_elements) | set(exclude_from_update)

        stmt = insert(model).values(data_list)

        update_cols = {
            col.name: stmt.excluded[col.name]
            for col in model.__table__.columns
            if col.name not in (excluded_cols)
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=index_elements,
            set_=update_cols
        )

        await db.execute(stmt)
        await db.commit()

        return len(data_list)
