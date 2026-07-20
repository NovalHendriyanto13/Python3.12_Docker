from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from datetime import datetime, timedelta
from app.models.dim_venues_model import DimVenues
from app.models.dim_date_model import DimDate
from app.models.fact_sales_summary import FactSalesSummary
from app.models.fact_consumer_sales import FactConsumerSales
from app.helpers.app_helper import to_int, to_bool, get_today_range, to_utc_datetime
from app.helpers.db_helper import _upsert_batch
from configs.mongo import mongo_conn

async def upsert_sales(db: AsyncSession, mongo_doc: dict, is_init: bool = False):
    start_datetime, end_datetime = get_today_range()
    await sales_summary(db, start_datetime, end_datetime)
    await consumer_sales(start_datetime, end_datetime)

    await db.commit()

async def get_dim_key(target_date: datetime):
    stmt = select(DimDate.date_key).where(DimDate.full_date == target_date)
    result = await db.execute(stmt)
    data_key = result.scalar_one_or_one()

    return data_key

async def sales_summary(
    db: AsyncSession,
    start_of_day: datetime, 
    end_of_day: datetime
):
    pipeline = [
        {
            "$match": {
                # "transactionsourcetimelocal": {"$gte": start_of_day, "$lt": end_of_day}
                "dateoccurred": {"$gte": to_utc_datetime(start_of_day), "$lt": to_utc_datetime(end_of_day)}
            }
        },
        {
            "$group": {
                "_id": None,
                "total_amount": {"$sum": "$totalamount"},
                "gross_amount": {"$sum": "$grossamount"},
                "tax_total_amount": {"$sum": "$taxtotalamount"},
                "before_discount_tax_total_amount": {"$sum": "$beforediscounttaxtotalamount"},
                "before_discount_total_amount": {"$sum": "$beforediscounttotalamount"},
            }
        }
    ]
    cursor = mongo_conn["mcd_sales_headers"].aggregate(pipeline)
    # cursor = mongo_conn["mcd_sales_headers"].find()
    agg_result = await cursor.to_list(length=1)
    print('================================', pipeline)
    if not agg_result:
        return None

    # summary = agg_result[0]
    # print(summary)

    # # get date key
    # date_key = get_dim_key(start_of_day)

    # data_values = {
    #     "date_key": date_key,
    #     "total_amount": summary.get("total_amount") or 0,
    #     "gross_amount": summary.get("gross_amount") or 0,
    #     "tax_total_amount": summary.get("tax_total_amount") or 0,
    #     "before_discount_tax_total_amount": summary.get("before_discount_tax_total_amount") or 0,
    #     "before_discount_total_amount": summary.get("before_discount_total_amount") or 0,
    # }

    # await _upsert_batch(
    #     db=db,
    #     model=FactSalesSummary,
    #     data_list=[data_values],
    #     index_elements=["date_key"],
    # )

    # return data_values
    return 0

async def consumer_sales(start_of_day: datetime, end_of_day: datetime):
    pipeline = [
        {
            "$match": {
                "saledate": {"$gte": start_of_day, "$lt": end_of_day}
            }
        },
        {
            "$group": {
                "_id": "$reporting_id",
                "total_visit": {"$sum": 1},
            }
        }
    ]
    cursor = mongo_conn["mcd_sale_headers"].aggregate(pipeline)
    agg_result = await cursor.to_list(length=1)

    if not agg_result:
        return None

    summary = agg_result[0]

    # get date key
    date_key = get_dim_key(start_of_day)

    data_values = {
        "date_key": date_key,
        "total_amount": summary.get("total_amount") or 0,
        "gross_amount": summary.get("gross_amount") or 0,
        "tax_total_amount": summary.get("tax_total_amount") or 0,
        "before_discount_tax_total_amount": summary.get("before_discount_tax_total_amount") or 0,
        "before_discount_total_amount": summary.get("before_discount_total_amount") or 0,
    }

    await _upsert_batch(
        db=db,
        model=FactSalesSummary,
        data_list=[data_values],
        index_elements=["date_key"],
    )

    return data_values