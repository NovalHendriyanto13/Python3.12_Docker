from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.sales_headers_model import SalesHeaders
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal

async def upsert_sales_headers(db: AsyncSession, mongo_doc: dict):
    stmt = insert(SalesHeaders).values(
        reporting_id = to_uuid(mongo_doc["reporting_id"]),                  
        sale_id = mongo_doc["sale_id"],
        total_amount = to_decimal(mongo_doc["total_amount"]),
        offer_id = mongo_doc["offer_id"],
        internal_id = mongo_doc["internal_id"],                         
        gross_amount = to_decimal(mongo_doc["gross_amount"]),
        tax_total_amount = to_decimal(mongo_doc["tax_total_amount"]),
        before_discount_tax_total_amount = to_decimal(mongo_doc["before_discount_tax_total_amount"]),
        before_discount_total_amount = to_decimal(mongo_doc["before_discount_total_amount"]),
        day_part = mongo_doc["day_part"],
        pod_type = mongo_doc["pod_type"],
        transaction_kind = mongo_doc["transaction_kind"],
        order_take_platform = mongo_doc["order_take_platform"],
        sale_type = mongo_doc["sale_type"],
        venue_id = mongo_doc["venue_id"], 
        date_occurred = to_datetime(mongo_doc["date_occurred"]), 
        pos_sales_transaction_id = to_uuid(mongo_doc["pos_sales_transaction_id"]),

    ).on_conflict_do_update(
        index_elements=["sale_id"],
        set_={

            "reporting_id": to_uuid(mongo_doc["reporting_id"]),                  
            "total_amount": to_decimal(mongo_doc["total_amount"]),
            "offer_id": mongo_doc["offer_id"],
            "internal_id": mongo_doc["internal_id"],                         
            "gross_amount": to_decimal(mongo_doc["gross_amount"]),
            "tax_total_amount": to_decimal(mongo_doc["tax_total_amount"]),
            "before_discount_tax_total_amount": to_decimal(mongo_doc["before_discount_tax_total_amount"]),
            "before_discount_total_amount": to_decimal(mongo_doc["before_discount_total_amount"]),
            "day_part": mongo_doc["day_part"],
            "pod_type": mongo_doc["pod_type"],
            "transaction_kind": mongo_doc["transaction_kind"],
            "order_take_platform": mongo_doc["order_take_platform"],
            "sale_type": mongo_doc["sale_type"],
            "venue_id": mongo_doc["venue_id"], 
            "date_occurred": to_datetime(mongo_doc["date_occurred"]), 
            "pos_sales_transaction_id": to_uuid(mongo_doc["pos_sales_transaction_id"]),
        }
    )
    await db.execute(stmt)
    await db.commit()
