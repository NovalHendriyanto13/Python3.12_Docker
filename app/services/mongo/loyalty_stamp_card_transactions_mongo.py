from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.loyalty_stamp_card_transactions_model import LoyaltyStampCardTransactions
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_loyalty_stamp_card_transactions(db: AsyncSession, mongo_doc: dict):
    stmt=insert(LoyaltyStampCardTransactions).values(
        stamp_card_transaction_id=to_uuid(mongo_doc.get('stamp_card_transaction_id')),
        pos_sales_transaction_id=to_uuid(mongo_doc.get('pos_sales_transaction_id')),
        transaction_time_utc=to_datetime(mongo_doc.get('transaction_time_utc')),
        transaction_source_time_local=to_datetime(mongo_doc.get('transaction_source_time_local')),
        transaction_source_time_utc_offset=mongo_doc.get('transaction_source_time_utc_offset'),
        transaction_origin=mongo_doc.get('transaction_origin'),
        is_initial_rewarded_stamps=to_bool(mongo_doc.get('is_initial_rewarded_stamps')),
        note=mongo_doc.get('note'),
        stamp_card_id=to_uuid(mongo_doc.get('stamp_card_id')),
        total_card_stamps_after_transaction=to_int(mongo_doc.get('total_card_stamps_after_transaction')),
        reporting_id=to_uuid(mongo_doc.get('reporting_id')),
        incentive_program_id=to_int(mongo_doc.get('incentive_program_id')),
        venue_id=mongo_doc.get('venue_id'),
        number_of_stamps_added=to_int(mongo_doc.get('number_of_stamps_added')),
        external_transaction_reference=mongo_doc.get('external_transaction_reference'),
        sale_id=mongo_doc.get('sale_id'),
        venue_external_id=mongo_doc.get('venue_external_id'),
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "stamp_card_transaction_id": to_uuid(mongo_doc.get('stamp_card_transaction_id')),
            "pos_sales_transaction_id": to_uuid(mongo_doc.get('pos_sales_transaction_id')),
            "transaction_time_utc": to_datetime(mongo_doc.get('transaction_time_utc')),
            "transaction_source_time_local": to_datetime(mongo_doc.get('transaction_source_time_local')),
            "transaction_source_time_utc_offset": mongo_doc.get('transaction_source_time_utc_offset'),
            "transaction_origin": mongo_doc.get('transaction_origin'),
            "is_initial_rewarded_stamps": to_bool(mongo_doc.get('is_initial_rewarded_stamps')),
            "note": mongo_doc.get('note'),
            "stamp_card_id": to_uuid(mongo_doc.get('stamp_card_id')),
            "total_card_stamps_after_transaction": to_int(mongo_doc.get('total_card_stamps_after_transaction')),
            "reporting_id": to_uuid(mongo_doc.get('reporting_id')),
            "incentive_program_id": to_int(mongo_doc.get('incentive_program_id')),
            "venue_id": mongo_doc.get('venue_id'),
            "number_of_stamps_added": to_int(mongo_doc.get('number_of_stamps_added')),
            "external_transaction_reference": mongo_doc.get('external_transaction_reference'),
            "sale_id": mongo_doc.get('sale_id'),
            "venue_external_id": mongo_doc.get('venue_external_id'),
        }
    )
    await db.execute(stmt)
    await db.commit()
