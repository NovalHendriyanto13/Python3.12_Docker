from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.points_program_transactions_model import PointsProgramTransactions
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_points_program_transactions(db: AsyncSession, mongo_doc: dict):
    points_program_transaction_id=to_uuid(mongo_doc.get('points_program_transaction_id'))
        
    existing = await db.execute(
        select(PointsProgramTransactions).where(
            PointsProgramTransactions.points_program_transaction_id == points_program_transaction_id
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        points_program_transaction_id=points_program_transaction_id,
        points_program_transaction_group_id=to_uuid(mongo_doc.get('points_program_transaction_group_id')),
        pos_sales_transaction_id=to_uuid(mongo_doc.get('pos_sales_transaction_id')),
        sale_id=to_uuid(mongo_doc.get('sale_id')),
        incentive_program_id=to_int(mongo_doc.get('incentive_program_id')),
        incentive_program_name=mongo_doc.get('incentive_program_name'),
        transaction_type=mongo_doc.get('transaction_type'),
        transaction_sub_type=mongo_doc.get('transaction_sub_type'),
        transaction_origin=mongo_doc.get('transaction_origin'),
        note=mongo_doc.get('note'),
        points_balance_after_transaction=to_int(mongo_doc.get('points_balance_after_transaction')),
        points_delta=to_int(mongo_doc.get('points_delta')),
        pointsrequested=to_int(mongo_doc.get('pointsrequested')),
        external_transaction_reference=mongo_doc.get('external_transaction_reference'),
        referenced_transactions=mongo_doc.get('referenced_transactions'),
        reward_type=mongo_doc.get('reward_type'),
        reward_reference_id=to_uuid(mongo_doc.get('reward_reference_id')),
        venue_id=to_int(mongo_doc.get('venue_id')),
        venue_name=mongo_doc.get('venue_name'),
        venue_external_id=mongo_doc.get('venue_external_id'),
        market=mongo_doc.get('market'),
        reporting_id=to_uuid(mongo_doc.get('reporting_id')),
        date=to_datetime(mongo_doc.get('date')),
        transaction_source_time_local=to_datetime(mongo_doc.get('transaction_source_time_local')),
        transaction_time_utc=to_datetime(mongo_doc.get('transaction_time_utc'))
    )

    stmt=insert(PointsProgramTransactions).values(**new_values).on_conflict_do_update(
        index_elements=["points_program_transaction_id"],
        set_=new_values
    )
    await db.execute(stmt)

    if existing_record is not None:
        StagingDataChangesService(
            db=db,
            module_name="mcd_points_program_transactions",
            module_id=points_program_transaction_id,
            old_data=existing_record,
            new_data=new_values
        )
        
    await db.commit()
