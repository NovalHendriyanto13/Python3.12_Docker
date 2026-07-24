from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from datetime import datetime, timedelta
from app.models.dim_venues_model import DimVenues
from app.models.dim_date_model import DimDate
from app.models.fact_sales_summary import FactSalesSummary
from app.models.fact_consumer_sales import FactConsumerSales
from app.helpers.app_helper import to_int, to_bool, get_today_range, to_iso_z
from app.helpers.db_helper import _upsert_batch, _get_dim_key_list
from app.helpers.mongo_helper import to_double_safe
from configs.mongo import mongo_conn

async def upsert_sales(db: AsyncSession, mongo_doc: dict, is_init: bool = False):
    start_datetime, end_datetime = get_today_range()
    try:
        await sales_summary(db, start_datetime, end_datetime)
        # await consumer_sales(db, start_datetime, end_datetime)
        
        await db.commit()
    except Exception:
        await db.rollback()
        raise

async def sales_summary(
    db: AsyncSession,
    start_of_day: datetime, 
    end_of_day: datetime
):
    pipeline = [
        {
            "$match": {
                # "transaction_source_time_local": {"$gte": to_iso_z(start_of_day), "$lt": to_iso_z(end_of_day)}
                "dateoccurred": {"$gte": to_iso_z(start_of_day), "$lt": to_iso_z(end_of_day)}
            }
        },
        {
            "$addFields": {
                "totalamount_num": to_double_safe("totalamount"),
                "grossamount_num": to_double_safe("grossamount"),
                "taxtotalamount_num": to_double_safe("taxtotalamount"),
                "beforediscounttaxtotalamount_num": to_double_safe("beforediscounttaxtotalamount"),
                "beforediscounttotalamount_num": to_double_safe("beforediscounttotalamount"),
                "date_only": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": {"$dateFromString": {"dateString": "$dateoccurred"}}
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "date": "$date_only"
                },
                "total_amount": {"$sum": "$totalamount_num"},
                "gross_amount": {"$sum": "$grossamount_num"},
                "tax_total_amount": {"$sum": "$taxtotalamount_num"},
                "before_discount_tax_total_amount": {"$sum": "$beforediscounttaxtotalamount_num"},
                "before_discount_total_amount": {"$sum": "$beforediscounttotalamount_num"},
            }
        }
    ]
    cursor = mongo_conn["mcd_sales_headers"].aggregate(pipeline)
    agg_result = await cursor.to_list()

    if not agg_result:
        return None

    unique_dates = sorted(set(r["_id"]["date"] for r in agg_result))
    
    # get date key
    date_keys = await _get_dim_key_list(db, unique_dates)
    
    dim_dates_map = {
        d.full_date.strftime("%Y-%m-%d") if hasattr(d.full_date, "strftime") else d.full_date: d
        for d in date_keys
    }
    matched_data = []
    for r in agg_result:
        date_only = r["_id"]["date"]
        dim_date = dim_dates_map.get(date_only)

        if dim_date is None:
            print(f"WARNING: no dim_date found for {date_only}")
            continue

        matched_data.append({
            "date_key": dim_date.date_key,  # adjust field name to match your DimDates model
            "total_amount": r["total_amount"],
            "gross_amount": r["gross_amount"],
            "tax_total_amount": r["tax_total_amount"],
            "before_discount_tax_total_amount": r["before_discount_tax_total_amount"],
            "before_discount_total_amount": r["before_discount_total_amount"],
        })

    if not matched_data:
        return None

    upsert = await _upsert_batch(
        db=db,
        model=FactSalesSummary,
        data_list=matched_data,
        index_elements=["date_key"],
        exclude_from_update=["sales_summary_key"]
    )

    return upsert

async def consumer_sales(
    db: AsyncSession,
    start_of_day: datetime, 
    end_of_day: datetime
):
    pipeline = [
        {
            "$match": {
                # "saledate": {"$gte": start_of_day, "$lt": end_of_day}
                "dateoccurred": {"$gte": to_iso_z(start_of_day), "$lt": to_iso_z(end_of_day)}
            }
        },
        {
            "$group": {
                "_id": "$reportingid",
                "total_visit": {"$sum": 1},
            }
        }
    ]
    print("---------pipeline", pipeline)
    cursor = mongo_conn["mcd_sale_headers"].aggregate(pipeline)
    agg_result = await cursor.to_list()

    print("=================", agg_result)

    # if not agg_result:
    #     return None

    # summary = agg_result[0]

    # # get date key
    # date_key = get_dim_key(start_of_day)

        if dim_date is None:
            print(f"WARNING: no dim_date found for {date_only}")
            continue

        matched_data.append({
            "date_key": dim_date.date_key,  # adjust field name to match your DimDates model
            "total_amount": r["total_amount"],
            "gross_amount": r["gross_amount"],
            "tax_total_amount": r["tax_total_amount"],
            "before_discount_tax_total_amount": r["before_discount_tax_total_amount"],
            "before_discount_total_amount": r["before_discount_total_amount"],
        })

    if not matched_data:
        return None

    await _upsert_batch(
        db=db,
        model=FactSalesSummary,
        data_list=matched_data,
        index_elements=["date_key"],
    )

    return matched_data

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