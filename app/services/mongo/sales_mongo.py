from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime
from app.models.dim_venues_model import DimVenues
from app.models.dim_date_model import DimDate
from app.models.dim_consumer_model import DimConsumers
from app.models.fact_sales_summary import FactSalesSummary
from app.models.fact_consumer_sales_summary import FactConsumerSalesSummary
from app.helpers.app_helper import to_int, to_bool, get_today_range, to_iso_z, _chunked, to_uuid
from app.helpers.db_helper import _upsert_batch, _get_dim_key_date_list
from app.helpers.mongo_helper import to_double_safe
from configs.mongo import mongo_conn

async def upsert_sales(db: AsyncSession, mongo_doc: dict, is_init: bool = False):
    start_datetime, end_datetime = get_today_range()
    try:
        await sales_summary(db, start_datetime, end_datetime)
        await consumer_sales_summay(db, start_datetime, end_datetime)
        
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
    cursor = mongo_conn["mcd_sale_headers"].aggregate(pipeline)
    agg_result = await cursor.to_list()

    if not agg_result:
        return None

    unique_dates = sorted(set(r["_id"]["date"] for r in agg_result))
    
    # get date key
    date_keys = await _get_dim_key_date_list(db, unique_dates)
    
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

async def consumer_sales_summay(
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
                    "date": "$date_only",
                    "venue_id": "$venueid",
                    "reporting_id": "$reportingid"
                },
                "total_amount": {"$sum": "$totalamount_num"},
                "gross_amount": {"$sum": "$grossamount_num"},
                "tax_total_amount": {"$sum": "$taxtotalamount_num"},
                "before_discount_tax_total_amount": {"$sum": "$beforediscounttaxtotalamount_num"},
                "before_discount_total_amount": {"$sum": "$beforediscounttotalamount_num"},
            }
        }
    ]

    cursor = mongo_conn["mcd_sale_headers"].aggregate(pipeline)
    agg_result = await cursor.to_list()

    if not agg_result:
        return None

    unique_dates = sorted(set(r["_id"]["date"] for r in agg_result))

    # get date key
    date_keys = await _get_dim_key_date_list(db, unique_dates)
    
    dim_dates_map = {
        d.full_date.strftime("%Y-%m-%d") if hasattr(d.full_date, "strftime") else d.full_date: d
        for d in date_keys
    }

    matched_data = []
    consumer_list = {}
    venue_list = {}
    for r in agg_result:
        date_only = r["_id"]["date"]
        dim_date = dim_dates_map.get(date_only)

        if dim_date is None:
            print(f"WARNING: no dim_date found for {date_only}")
            continue

        # consumer part
        reporting_id = to_uuid(r["_id"]["reporting_id"])
        if reporting_id in consumer_list:
            consumer_key = consumer_list[reporting_id].reporting_id
        else:
            consumers = await db.execute(
                select(DimConsumers).where(
                    DimConsumers.reporting_id == to_uuid(r["_id"]["reporting_id"])
                )
            )

            consumer = consumers.scalar_one_or_none()
            if consumer:
                consumer_list[reporting_id] = consumer
                consumer_key = consumer.reporting_id
            else:
                consumer_key = None

        # venues part
        venue_id = r["_id"]["venue_id"]
        if venue_id in venue_list:
            venue_key = venue_list[venue_id].venue_key
        else:
            venues = await db.execute(
                select(DimVenues).where(
                    DimVenues.venue_key == to_int(r["_id"]["venue_id"])
                )
            )

            venue = venues.scalar_one_or_none()
            if venue:
                venue_list[venue_id] = venue
                venue_key = venue.venue_key
            else:
                venue_key = None

        matched_data.append({
            "date_key": dim_date.date_key,  # adjust field name to match your DimDates model
            "consumer_key": consumer_key,
            "venue_key": venue_key,
            "total_amount": r["total_amount"],
            "gross_amount": r["gross_amount"],
            "tax_total_amount": r["tax_total_amount"],
            "before_discount_tax_total_amount": r["before_discount_tax_total_amount"],
            "before_discount_total_amount": r["before_discount_total_amount"],
        })

    if not matched_data:
        return None

    columns_per_row = len(matched_data[0])
    safe_param_limit = 30000
    chunk_size = max(1, safe_param_limit // columns_per_row)

    results = []
    try:
        for chunk in _chunked(matched_data, chunk_size):
            upsert = await _upsert_batch(
                db=db,
                model=FactConsumerSalesSummary,
                data_list=chunk,
                index_elements=["date_key", "venue_key", "consumer_key"],
                exclude_from_update=["consumer_sales_summary_key"]
            )
            results.append(upsert)
            await db.commit()
    except Exception:
        await db.rollback()
        raise 
    
    return results


async def consumer_visit(
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
            "$addFields": {
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
                    "date": "$date_only",
                    "reporting_id": "$reportingid"
                },
                "total_visit": {"$sum": 1},
            }
        }
    ]
    cursor = mongo_conn["mcd_sale_headers"].aggregate(pipeline)
    agg_result = await cursor.to_list()

    if not agg_result:
        return None

    unique_dates = sorted(set(r["_id"]["date"] for r in agg_result))
    
    # get date key
    date_keys = await _get_dim_key_date_list(db, unique_dates)
    
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
            "total_visit": r["total_visit"],
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