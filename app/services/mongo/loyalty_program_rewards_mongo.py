from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.loyalty_program_rewards_model import LoyaltyProgramRewards
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_loyalty_program_rewards(db: AsyncSession, mongo_doc: dict):
    id=to_int(mongo_doc.get('id'))
    
    existing = await db.execute(
        select(LoyaltyProgramRewards).where(
            LoyaltyProgramRewards.id == id
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        id=id,
        loyalty_program_id=to_int(mongo_doc.get('loyalty_program_id')),
        reward_type=to_int(mongo_doc.get('reward_type')),
        offer_id=to_int(mongo_doc.get('offer_id')),
        expires_after_n_days=to_int(mongo_doc.get('expires_after_n_days')),
        is_expiry_time_specified=to_bool(mongo_doc.get('is_expiry_time_specified')),
        expiry_time_after_activation=to_int(mongo_doc.get('expiry_time_after_activation')),
        point_value=to_int(mongo_doc.get('point_value')),
        activation_limit=to_int(mongo_doc.get('activation_limit')),
        activation_weight=to_int(mongo_doc.get('activation_weight')),
        activation_remained=to_int(mongo_doc.get('activation_remained')),
        market=mongo_doc.get('market')
    )

    stmt=insert(LoyaltyProgramRewards).values(**new_values).on_conflict_do_update(
        index_elements=["id"],
        set_=new_values
    )
    await db.execute(stmt)

    if existing_record is not None:
        StagingDataChangesService(
            db=db,
            module_name="mcd_loyalty_program_rewards",
            module_id=id,
            old_data=existing_record,
            new_data=new_values
        )
        
    await db.commit()
