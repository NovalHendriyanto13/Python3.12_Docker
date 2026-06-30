from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.sales_headers_model import SalesHeaders
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_sale_headers(db: AsyncSession, mongo_doc: dict):
    sale_id=to_uuid(mongo_doc.get('sale_id'))
    
    existing = await db.execute(
        select(SalesHeaders).where(
            SalesHeaders.sale_id == sale_id
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        sale_id=sale_id
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
        plexure_processing_time_utc=to_datetime(mongo_doc.get('plexure_processing_time_utc'))
    )

    stmt=insert(SalesHeaders).values(**new_values).on_conflict_do_update(
        index_elements=["sale_id"],
        set_=new_values
    )
    await db.execute(stmt)

    if existing_record is not None:
        StagingDataChangesService(
            db=db,
            module_name="mcd_sale_headers",
            module_id=sale_id,
            old_data=existing_record,
            new_data=new_values
        )
        
    await db.commit()
