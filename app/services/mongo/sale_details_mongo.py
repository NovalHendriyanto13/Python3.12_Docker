from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.sale_details_model import SaleDetails
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_sale_details(db: AsyncSession, mongo_doc: dict):
    pos_transaction_id=to_uuid(mongo_doc.get('pos_transaction_id'))
    sale_id=to_uuid(mongo_doc.get('sale_id'))
    reporting_id=to_uuid(mongo_doc.get('reporting_id'))
    
    existing = await db.execute(
        select(SaleDetails).where(
            SaleDetails.pos_transaction_id == pos_transaction_id,
            SaleDetails.sale_id == sale_id,
            SaleDetails.reporting_id == reporting_id
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        pos_transaction_id=pos_transaction_id,
        sale_id=sale_id,
        reporting_id=reporting_id,
        offer_id=to_int(mongo_doc.get('offer_id')),
        external_venue_id=to_uuid(mongo_doc.get('external_venue_id')),
        market=mongo_doc.get('market'),
        product_code=mongo_doc.get('product_code'),
        quantity=to_int(mongo_doc.get('quantity')),
        unit_price=to_decimal(mongo_doc.get('unit_price')),
        net_unit_price=to_decimal(mongo_doc.get('net_unit_price')),
        tax_unit_amount=to_decimal(mongo_doc.get('tax_unit_amount')),
        line_item_number=mongo_doc.get('line_item_number'),
        date=to_datetime(mongo_doc.get('date')),
        transaction_source_time_local=to_datetime(mongo_doc.get('transaction_source_time_local'))
    )

    stmt=insert(SaleDetails).values(**new_values).on_conflict_do_update(
        index_elements=["pos_transaction_id", "sale_id", "reporting_id"],
        set_=new_values
    )
    await db.execute(stmt)

    if existing_record is not None:
        StagingDataChangesService(
            db = db,
            module_name = "mcd_sale_details",
            module_id = (pos_transaction_id + ":" + sale_id + ":" + reporting_id),
            old_data = existing_record,
            new_data = new_values
        )
        
    await db.commit()
