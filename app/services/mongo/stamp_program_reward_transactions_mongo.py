from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.stamp_program_reward_transactions_model import StampProgramRewardTransactions
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_stamp_program_reward_transactions(db: AsyncSession, mongo_doc: dict):
    stamp_program_reward_transaction_id=to_uuid(mongo_doc.get('stamp_program_reward_transaction_id'))
    
    existing = await db.execute(
        select(StampProgramRewardTransactions).where(
            StampProgramRewardTransactions.stamp_program_reward_transaction_id == stamp_program_reward_transaction_id
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        stamp_program_reward_transaction_id=stamp_program_reward_transaction_id,
        stamp_program_reward_transaction_group_id=to_uuid(mongo_doc.get('stamp_program_reward_transaction_group_id')),
        pos_sales_transaction_id=to_uuid(mongo_doc.get('pos_sales_transaction_id')),
        sale_id=to_uuid(mongo_doc.get('sale_id')),
        incentive_program_id=to_int(mongo_doc.get('incentive_program_id')),
        incentive_program_name=mongo_doc.get('incentive_program_name'),
        stamp_reward_transaction_type=mongo_doc.get('stamp_reward_transaction_type'),
        stamp_card_id=mongo_doc.get('stamp_card_id'),
        card_capacity=to_int(mongo_doc.get('card_capacity')),
        reward_type=mongo_doc.get('reward_type'),
        reward_reference_id=mongo_doc.get('reward_reference_id'),
        venue_id=to_int(mongo_doc.get('venue_id')),
        venue_name=mongo_doc.get('venue_name'),
        venue_external_id=mongo_doc.get('venue_external_id'),
        market=mongo_doc.get('market'),
        reporting_id=to_uuid(mongo_doc.get('reporting_id')),
        date=to_datetime(mongo_doc.get('date')),
        transaction_source_time_local=to_datetime(mongo_doc.get('transaction_source_time_local')),
        transaction_time_utc=to_datetime(mongo_doc.get('transaction_time_utc'))
    )

    stmt=insert(StampProgramRewardTransactions).values(**new_values).on_conflict_do_update(
        index_elements=["stamp_program_reward_transaction_id"],
        set_=new_values
    )
    await db.execute(stmt)

    if existing_record is not None:
        StagingDataChangesService(
            db=db,
            module_name="mcd_stamp_program_reward_transactions",
            module_id=stamp_program_reward_transaction_id,
            old_data=existing_record,
            new_data=new_values
        )
        
    await db.commit()
