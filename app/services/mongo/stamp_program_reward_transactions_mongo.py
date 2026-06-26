from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.stamp_program_reward_transactions_model import StampProgramRewardTransactions
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_stamp_program_reward_transactions(db: AsyncSession, mongo_doc: dict):
    stmt=insert(StampProgramRewardTransactions).values(
        stamp_program_reward_transaction_id=to_uuid(mongo_doc.get('stamp_program_reward_transaction_id')),
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
        transaction_time_utc=to_datetime(mongo_doc.get('transaction_time_utc')),
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "stamp_program_reward_transaction_id": to_uuid(mongo_doc.get('stamp_program_reward_transaction_id')),
            "stamp_program_reward_transaction_group_id": to_uuid(mongo_doc.get('stamp_program_reward_transaction_group_id')),
            "pos_sales_transaction_id": to_uuid(mongo_doc.get('pos_sales_transaction_id')),
            "sale_id": to_uuid(mongo_doc.get('sale_id')),
            "incentive_program_id": to_int(mongo_doc.get('incentive_program_id')),
            "incentive_program_name": mongo_doc.get('incentive_program_name'),
            "stamp_reward_transaction_type": mongo_doc.get('stamp_reward_transaction_type'),
            "stamp_card_id": mongo_doc.get('stamp_card_id'),
            "card_capacity": to_int(mongo_doc.get('card_capacity')),
            "reward_type": mongo_doc.get('reward_type'),
            "reward_reference_id": mongo_doc.get('reward_reference_id'),
            "venue_id": to_int(mongo_doc.get('venue_id')),
            "venue_name": mongo_doc.get('venue_name'),
            "venue_external_id": mongo_doc.get('venue_external_id'),
            "market": mongo_doc.get('market'),
            "reporting_id": to_uuid(mongo_doc.get('reporting_id')),
            "date": to_datetime(mongo_doc.get('date')),
            "transaction_source_time_local": to_datetime(mongo_doc.get('transaction_source_time_local')),
            "transaction_time_utc": to_datetime(mongo_doc.get('transaction_time_utc')),
        }
    )
    await db.execute(stmt)
    await db.commit()
