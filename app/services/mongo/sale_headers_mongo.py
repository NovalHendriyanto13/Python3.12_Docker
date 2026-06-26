from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.sales_headers_model import SalesHeaders
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_sale_headers(db: AsyncSession, mongo_doc: dict):
    stmt=insert(SalesHeaders).values(
        sale_id=to_uuid(mongo_doc.get('sale_id')),
        pos_transaction_id=to_uuid(mongo_doc.get('pos_transaction_id')),
        reporting_id=to_uuid(mongo_doc.get('reporting_id')),
        offer_ids=mongo_doc.get('offer_ids'),
        venue_external_id=mongo_doc.get('venue_external_id'),
        total_amount=to_decimal(mongo_doc.get('total_amount')),
        tax_total_amount=to_decimal(mongo_doc.get('tax_total_amount')),
        gross_amount=to_decimal(mongo_doc.get('gross_amount')),
        before_discount_tax_total_amount=to_decimal(mongo_doc.get('before_discount_tax_total_amount')),
        before_discount_total_amount=to_decimal(mongo_doc.get('before_discount_total_amount')),
        day_part=mongo_doc.get('day_part'),
        pod_type=mongo_doc.get('pod_type'),
        transaction_kind=mongo_doc.get('transaction_kind'),
        order_take_platform=mongo_doc.get('order_take_platform'),
        sale_type=mongo_doc.get('sale_type'),
        date=to_datetime(mongo_doc.get('date')),
        market=mongo_doc.get('market'),
        transaction_source_time_local=to_datetime(mongo_doc.get('transaction_source_time_local')),
        plexure_processing_time_utc=to_datetime(mongo_doc.get('plexure_processing_time_utc')),
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "sale_id": to_uuid(mongo_doc.get('sale_id')),
            "pos_transaction_id": to_uuid(mongo_doc.get('pos_transaction_id')),
            "reporting_id": to_uuid(mongo_doc.get('reporting_id')),
            "offer_ids": mongo_doc.get('offer_ids'),
            "venue_external_id": mongo_doc.get('venue_external_id'),
            "total_amount": to_decimal(mongo_doc.get('total_amount')),
            "tax_total_amount": to_decimal(mongo_doc.get('tax_total_amount')),
            "gross_amount": to_decimal(mongo_doc.get('gross_amount')),
            "before_discount_tax_total_amount": to_decimal(mongo_doc.get('before_discount_tax_total_amount')),
            "before_discount_total_amount": to_decimal(mongo_doc.get('before_discount_total_amount')),
            "day_part": mongo_doc.get('day_part'),
            "pod_type": mongo_doc.get('pod_type'),
            "transaction_kind": mongo_doc.get('transaction_kind'),
            "order_take_platform": mongo_doc.get('order_take_platform'),
            "sale_type": mongo_doc.get('sale_type'),
            "date": to_datetime(mongo_doc.get('date')),
            "market": mongo_doc.get('market'),
            "transaction_source_time_local": to_datetime(mongo_doc.get('transaction_source_time_local')),
            "plexure_processing_time_utc": to_datetime(mongo_doc.get('plexure_processing_time_utc')),
        }
    )
    await db.execute(stmt)
    await db.commit()
