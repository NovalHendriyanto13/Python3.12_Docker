from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.loyalty_program_rewards_model import LoyaltyProgramRewards
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_loyalty_program_rewards(db: AsyncSession, mongo_doc: dict):
    stmt=insert(LoyaltyProgramRewards).values(
        id=to_int(mongo_doc.get('id')),
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
        market=mongo_doc.get('market'),
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "id": to_int(mongo_doc.get('id')),
            "loyalty_program_id": to_int(mongo_doc.get('loyalty_program_id')),
            "reward_type": to_int(mongo_doc.get('reward_type')),
            "offer_id": to_int(mongo_doc.get('offer_id')),
            "expires_after_n_days": to_int(mongo_doc.get('expires_after_n_days')),
            "is_expiry_time_specified": to_bool(mongo_doc.get('is_expiry_time_specified')),
            "expiry_time_after_activation": to_int(mongo_doc.get('expiry_time_after_activation')),
            "point_value": to_int(mongo_doc.get('point_value')),
            "activation_limit": to_int(mongo_doc.get('activation_limit')),
            "activation_weight": to_int(mongo_doc.get('activation_weight')),
            "activation_remained": to_int(mongo_doc.get('activation_remained')),
            "market": mongo_doc.get('market'),
        }
    )
    await db.execute(stmt)
    await db.commit()
