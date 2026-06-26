from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.sale_details_model import SaleDetails
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_sale_details(db: AsyncSession, mongo_doc: dict):
    stmt=insert(SaleDetails).values(
        pos_transaction_id=to_uuid(mongo_doc.get('pos_transaction_id')),
        sale_id=to_uuid(mongo_doc.get('sale_id')),
        reporting_id=to_uuid(mongo_doc.get('reporting_id')),
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
        transaction_source_time_local=to_datetime(mongo_doc.get('transaction_source_time_local')),
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "pos_transaction_id": to_uuid(mongo_doc.get('pos_transaction_id')),
            "sale_id": to_uuid(mongo_doc.get('sale_id')),
            "reporting_id": to_uuid(mongo_doc.get('reporting_id')),
            "offer_id": to_int(mongo_doc.get('offer_id')),
            "external_venue_id": to_uuid(mongo_doc.get('external_venue_id')),
            "market": mongo_doc.get('market'),
            "product_code": mongo_doc.get('product_code'),
            "quantity": to_int(mongo_doc.get('quantity')),
            "unit_price": to_decimal(mongo_doc.get('unit_price')),
            "net_unit_price": to_decimal(mongo_doc.get('net_unit_price')),
            "tax_unit_amount": to_decimal(mongo_doc.get('tax_unit_amount')),
            "line_item_number": mongo_doc.get('line_item_number'),
            "date": to_datetime(mongo_doc.get('date')),
            "transaction_source_time_local": to_datetime(mongo_doc.get('transaction_source_time_local')),
        }
    )
    await db.execute(stmt)
    await db.commit()
