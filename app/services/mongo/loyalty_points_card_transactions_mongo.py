from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.loyalty_points_card_transactions_model import LoyaltyPointsCardTransactions

async def upsert_loyalty_points_card_transactions(db: AsyncSession, mongo_doc: dict):
    stmt = insert(LoyaltyPointsCardTransactions).values(

        points_card_transaction_id = mongo_doc["points_card_transaction_id"],
        points_card_transaction_group_id = mongo_doc["points_card_transaction_group_id"],
        pos_sales_transaction_id = mongo_doc["pos_sales_transaction_id"],
        transaction_time_utc = mongo_doc["transaction_time_utc"],
        transaction_source_time_local = mongo_doc["transaction_source_time_local"],
        transaction_source_time_utc_offset = mongo_doc["transaction_source_time_utc_offset"],
        transaction_type = mongo_doc["transaction_type"],
        transaction_sub_type = mongo_doc["transaction_sub_type"],
        transaction_origin = mongo_doc["transaction_origin"],
        note = mongo_doc["note"],
        points_balance_after_transaction = mongo_doc["points_balance_after_transaction"],
        reward_type = mongo_doc["reward_type"],
        reporting_id = mongo_doc["reporting_id"],
        incentive_program_id = mongo_doc["incentive_program_id"],
        venue_id = mongo_doc["venue_id"],
        referenced_transactions = mongo_doc["referenced_transactions"],
        points_delta = mongo_doc["points_delta"],
        external_transaction_reference   = mongo_doc["external_transaction_reference"],
        sale_id = mongo_doc["sale_id"],
        venue_external_id = mongo_doc["venue_external_id"],
        bonus_points_total = mongo_doc["bonus_points_total"],

    ).on_conflict_do_update(
        index_elements=["points_card_transaction_id"],
        set_={
            "points_card_transaction_group_id": mongo_doc["points_card_transaction_group_id"],
            "pos_sales_transaction_id": mongo_doc["pos_sales_transaction_id"],
            "transaction_time_utc": mongo_doc["transaction_time_utc"],
            "transaction_source_time_local": mongo_doc["transaction_source_time_local"],
            "transaction_source_time_utc_offset": mongo_doc["transaction_source_time_utc_offset"],
            "transaction_type": mongo_doc["transaction_type"],
            "transaction_sub_type": mongo_doc["transaction_sub_type"],
            "transaction_origin": mongo_doc["transaction_origin"],
            "note": mongo_doc["note"],
            "points_balance_after_transaction": mongo_doc["points_balance_after_transaction"],
            "reward_type": mongo_doc["reward_type"],
            "reporting_id": mongo_doc["reporting_id"],
            "incentive_program_id": mongo_doc["incentive_program_id"],
            "venue_id": mongo_doc["venue_id"],
            "referenced_transactions": mongo_doc["referenced_transactions"],
            "points_delta": mongo_doc["points_delta"],
            "external_transaction_reference  ": mongo_doc["external_transaction_reference"],
            "sale_id": mongo_doc["sale_id"],
            "venue_external_id": mongo_doc["venue_external_id"],
            "bonus_points_total": mongo_doc["bonus_points_total"],
        }
    )
    await db.execute(stmt)
    await db.commit()
