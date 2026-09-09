from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.offers_model import Offers
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_offers(db: AsyncSession, mongo_doc: dict):
    # mcd_offers Mongo docs only carry these flat, non-snake_case fields:
    # id, campaignid, category, codetype, description, isreward, redemptiontext,
    # status, title, whenexpires, whenlastupdated, whenstarts. Every other
    # column below has no source field and stays NULL.
    offer_id=to_int(mongo_doc.get('id'))
    
    existing = await db.execute(
        select(Offers).where(
            Offers.offer_id == offer_id
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        offer_id=offer_id,
        campaign_id=to_int(mongo_doc.get('campaignid')),
        category_id=to_int(mongo_doc.get('category_id')),
        category=mongo_doc.get('category'),
        market=mongo_doc.get('market'),
        title=mongo_doc.get('title'),
        description=mongo_doc.get('description'),
        terms_and_conditions=mongo_doc.get('terms_and_conditions'),
        has_barcode_image=to_bool(mongo_doc.get('has_barcode_image')),
        redemption_limit=to_int(mongo_doc.get('redemption_limit')),
        payment_type=to_int(mongo_doc.get('payment_type')),
        redemption_count_unlimited=to_bool(mongo_doc.get('redemption_count_unlimited')),
        code_type=to_int(mongo_doc.get('codetype')),
        discount_percent=to_decimal(mongo_doc.get('discount_percent')),
        discount_value=to_decimal(mongo_doc.get('discount_value')),
        status=to_int(mongo_doc.get('status')),
        apply_geo_fence_filters=to_bool(mongo_doc.get('apply_geo_fence_filters')),
        base_weight=to_int(mongo_doc.get('base_weight')),
        no_compete_group=mongo_doc.get('no_compete_group'),
        is_giftable=to_bool(mongo_doc.get('is_giftable')),
        is_reward=to_bool(mongo_doc.get('isreward')),
        is_respawning=to_bool(mongo_doc.get('is_respawning')),
        respawns_in_days=to_int(mongo_doc.get('respawns_in_days')),
        enable_distance_weight=to_bool(mongo_doc.get('enable_distance_weight')),
        is_available_all_stores=to_bool(mongo_doc.get('is_available_all_stores')),
        promotional_image_description=mongo_doc.get('promotional_image_description'),
        limit=to_int(mongo_doc.get('limit')),
        code_expiry_in_minutes=to_int(mongo_doc.get('code_expiry_in_minutes')),
        is_sticky=to_bool(mongo_doc.get('is_sticky')),
        sticky_expiration_days=to_int(mongo_doc.get('sticky_expiration_days')),
        sticky_expiration_time_of_day=to_int(mongo_doc.get('sticky_expiration_time_of_day')),
        offer_type=mongo_doc.get('offer_type'),
        respawn_start_time=to_int(mongo_doc.get('respawn_start_time')),
        name=mongo_doc.get('name'),
        respawns_in_minutes=to_int(mongo_doc.get('respawns_in_minutes')),
        consumer_redemption_limit=to_int(mongo_doc.get('consumer_redemption_limit')),
        redemption_text=mongo_doc.get('redemptiontext'),
        days_of_week=mongo_doc.get('days_of_week'),
        daily_start_time=to_int(mongo_doc.get('daily_start_time')),
        daily_end_time=to_int(mongo_doc.get('daily_end_time')),
        offer_start_time=to_datetime(mongo_doc.get('whenstarts')),
        offer_expire_time=to_datetime(mongo_doc.get('whenexpires')),
        when_last_updated_utc=to_datetime(mongo_doc.get('whenlastupdated'))
    )

    stmt=insert(Offers).values(**new_values).on_conflict_do_update(
        index_elements=["offer_id"],
        set_=new_values
    )
    await db.execute(stmt)

    if existing_record is not None:
        StagingDataChangesService(
            db=db,
            module_name="mcd_offers",
            module_id=offer_id,
            old_data=existing_record,
            new_data=new_values
        )
        
    await db.commit()
