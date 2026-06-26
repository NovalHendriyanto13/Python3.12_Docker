from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.bonus_points_breakdown_model import BonusPointsBreakdown
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_bonus_points_breakdown(db: AsyncSession, mongo_doc: dict):
    stmt=insert(BonusPointsBreakdown).values(
        points_program_transaction_id=to_uuid(mongo_doc.get('points_program_transaction_id')),
        pos_sales_transaction_id=mongo_doc.get('pos_sales_transaction_id'),
        ordering_method=mongo_doc.get('ordering_method'),
        day_of_week=mongo_doc.get('day_of_week'),
        time_of_day=mongo_doc.get('time_of_day'),
        minimum_spend=to_decimal(mongo_doc.get('minimum_spend')),
        product=mongo_doc.get('product'),
        satisfied_condition_type=mongo_doc.get('satisfied_condition_type'),
        bonus_rule_id=mongo_doc.get('bonus_rule_id'),
        bonus_points=to_int(mongo_doc.get('bonus_points')),
        standard_points=to_int(mongo_doc.get('standard_points')),
        venue_id=to_int(mongo_doc.get('venue_id')),
        venue_name=mongo_doc.get('venue_name'),
        venue_external_id=mongo_doc.get('venue_external_id'),
        market=mongo_doc.get('market'),
        reporting_id=to_uuid(mongo_doc.get('reporting_id')),
        date=to_datetime(mongo_doc.get('date')),
        transaction_source_time_local=to_datetime(mongo_doc.get('transaction_source_time_local')),
        transaction_time_utc=to_datetime(mongo_doc.get('transaction_time_utc')),
        bonus_points_breakdown_position=to_int(mongo_doc.get('bonus_points_breakdown_position')),
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "points_program_transaction_id": to_uuid(mongo_doc.get('points_program_transaction_id')),
            "pos_sales_transaction_id": mongo_doc.get('pos_sales_transaction_id'),
            "ordering_method": mongo_doc.get('ordering_method'),
            "day_of_week": mongo_doc.get('day_of_week'),
            "time_of_day": mongo_doc.get('time_of_day'),
            "minimum_spend": to_decimal(mongo_doc.get('minimum_spend')),
            "product": mongo_doc.get('product'),
            "satisfied_condition_type": mongo_doc.get('satisfied_condition_type'),
            "bonus_rule_id": mongo_doc.get('bonus_rule_id'),
            "bonus_points": to_int(mongo_doc.get('bonus_points')),
            "standard_points": to_int(mongo_doc.get('standard_points')),
            "venue_id": to_int(mongo_doc.get('venue_id')),
            "venue_name": mongo_doc.get('venue_name'),
            "venue_external_id": mongo_doc.get('venue_external_id'),
            "market": mongo_doc.get('market'),
            "reporting_id": to_uuid(mongo_doc.get('reporting_id')),
            "date": to_datetime(mongo_doc.get('date')),
            "transaction_source_time_local": to_datetime(mongo_doc.get('transaction_source_time_local')),
            "transaction_time_utc": to_datetime(mongo_doc.get('transaction_time_utc')),
            "bonus_points_breakdown_position": to_int(mongo_doc.get('bonus_points_breakdown_position')),
        }
    )
    await db.execute(stmt)
    await db.commit()
