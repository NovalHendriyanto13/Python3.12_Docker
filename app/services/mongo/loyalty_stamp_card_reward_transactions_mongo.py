from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.loyalty_stamp_card_reward_transactions_model import LoyaltyStampCardRewardTransactions
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_bool

async def upsert_loyalty_stamp_card_reward_transactions(db: AsyncSession, mongo_doc: dict):
    stmt = insert(LoyaltyStampCardRewardTransactions).values(

        stamp_card_reward_transaction_id = to_uuid(mongo_doc["stamp_card_reward_transaction_id"]),
        stamp_card_reward_transaction_group_id = to_uuid(mongo_doc["stamp_card_reward_transaction_group_id"]),
        pos_sales_transaction_id = to_uuid(mongo_doc["pos_sales_transaction_id"]),
        transaction_time_utc = to_datetime(mongo_doc["transaction_time_utc"]),     
        transaction_source_time_local = to_datetime(mongo_doc["transaction_source_time_local"]),
        transaction_source_time_utc_offset = mongo_doc["transaction_source_time_utc_offset"],
        stamp_reward_transaction_type = mongo_doc["stamp_reward_transaction_type"],
        note = mongo_doc["note"],                        
        stamp_card_id = to_uuid(mongo_doc["stamp_card_id"]),                    
        reporting_id = to_uuid(mongo_doc["reporting_id"]),                  
        incentive_program_id = to_int(mongo_doc["incentive_program_id"]), 
        venue_id = mongo_doc["venue_id"],                      
        sale_id = mongo_doc["sale_id"],
        venue_external_id = mongo_doc["venue_external_id"],

    ).on_conflict_do_update(
        index_elements=["stamp_card_reward_transaction_id"],
        set_={

            "stamp_card_reward_transaction_id": to_uuid(mongo_doc["stamp_card_reward_transaction_id"]),
            "stamp_card_reward_transaction_group_id": to_uuid(mongo_doc["stamp_card_reward_transaction_group_id"]),
            "pos_sales_transaction_id": to_uuid(mongo_doc["pos_sales_transaction_id"]),
            "transaction_time_utc": to_datetime(mongo_doc["transaction_time_utc"]),     
            "transaction_source_time_local": to_datetime(mongo_doc["transaction_source_time_local"]),
            "transaction_source_time_utc_offset": mongo_doc["transaction_source_time_utc_offset"],
            "stamp_reward_transaction_type": mongo_doc["stamp_reward_transaction_type"],
            "note": mongo_doc["note"],                        
            "stamp_card_id": to_uuid(mongo_doc["stamp_card_id"]),                    
            "reporting_id": to_uuid(mongo_doc["reporting_id"]),                  
            "incentive_program_id": to_int(mongo_doc["incentive_program_id"]), 
            "venue_id": mongo_doc["venue_id"],                      
            "sale_id": mongo_doc["sale_id"],
            "venue_external_id": mongo_doc["venue_external_id"],

        }
    )
    await db.execute(stmt)
    await db.commit()
