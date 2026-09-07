from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime
from app.models.dim_venues_model import DimVenues
from app.models.dim_date_model import DimDate
from app.models.dim_consumer_model import DimConsumers
from app.models.fact_sales_summary import FactSalesSummary
from app.models.fact_consumer_sales_summary import FactConsumerSalesSummary
from app.models.fact_consumer_sales_hourly_summary import FactConsumerSalesHourlySummary
from app.models.fact_consumer_sales_header_summary import FactConsumerSalesHeaderSummary
from app.helpers.app_helper import to_int, to_bool, get_today_range, to_iso_z, _chunked, to_uuid, to_decimal
from app.helpers.db_helper import _upsert_batch, _get_dim_key_date_list, _determine_transaction_type
from app.helpers.mongo_helper import to_double_safe
from configs.mongo import mongo_conn

import time

async def upsert_sales(db: AsyncSession, mongo_doc: dict, is_init: bool = False):
    start_datetime, end_datetime = get_today_range()
    try:
        await sales_summary(db, start_datetime, end_datetime)
        time.sleep(5)
        await consumer_sales_summary(db, start_datetime, end_datetime)
        time.sleep(5)
        await consumer_sales_header_summary(db, start_datetime, end_datetime)
        time.sleep(5)
        await consumer_sales_hourly_summary(db, start_datetime, end_datetime)

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
                "dateoccurred": {"$gte": to_iso_z(start_of_day), "$lt": to_iso_z(end_of_day)},
                # "$or": [
                #     {"transactionkind": "Discount"},
                #     {
                #         "transactionkind": "Sale",
                #         "$and": [
                #             {"offerids": {"$in": [None, ""]}}
                #         ]
                #     }
                # ]
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
                "transaction_count": {"$sum": 1},
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
            "transaction_count": r["transaction_count"],
        })

    if not matched_data:
        return None

    results = []
    try:
        for chunk in _chunked(matched_data, 1000):
            upsert = await _upsert_batch(
                db=db,
                model=FactSalesSummary,
                data_list=chunk,
                index_elements=["date_key"],
                exclude_from_update=["sales_summary_key"]
            )
            results.append(upsert)
            await db.commit()
    except Exception:
        await db.rollback()
        raise

    return results

async def consumer_sales_summary(
    db: AsyncSession,
    start_of_day: datetime,
    end_of_day: datetime
):
    pipeline = [
        {
            "$match": {
                # "transaction_source_time_local": {"$gte": to_iso_z(start_of_day), "$lt": to_iso_z(end_of_day)}
                "dateoccurred": {"$gte": to_iso_z(start_of_day), "$lt": to_iso_z(end_of_day)},
                # "$or": [
                #     {"transactionkind": "Discount"},
                #     {
                #         "transactionkind": "Sale",
                #         "$and": [
                #             {"offerids": {"$in": [None, ""]}}
                #         ]
                #     }
                # ]
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
                    "reporting_id": "$reportingid",
                },
                "total_amount": {"$sum": "$totalamount_num"},
                "gross_amount": {"$sum": "$grossamount_num"},
                "tax_total_amount": {"$sum": "$taxtotalamount_num"},
                "before_discount_tax_total_amount": {"$sum": "$beforediscounttaxtotalamount_num"},
                "before_discount_total_amount": {"$sum": "$beforediscounttotalamount_num"},
                "transaction_count": {"$sum": 1},
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
                continue

        # venues part
        venue_id = r["_id"]["venue_id"]
        if venue_id in venue_list:
            venue_key = venue_list[venue_id].venue_key
        else:
            venues = await db.execute(
                select(DimVenues).where(
                    DimVenues.venue_external_id == (r["_id"]["venue_id"])
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
            "transaction_count": r["transaction_count"],
        })

    if not matched_data:
        return None

    results = []
    try:
        for chunk in _chunked(matched_data, 1000):
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

async def consumer_sales_hourly_summary(
    db: AsyncSession,
    start_of_day: datetime,
    end_of_day: datetime
):
    pipeline = [
        {
            "$match": {
                "dateoccurred": {"$gte": to_iso_z(start_of_day), "$lt": to_iso_z(end_of_day)},
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
                },
                "hour_only": {
                    "$hour": {"$dateFromString": {"dateString": "$dateoccurred"}}
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "date": "$date_only",
                    "hour": "$hour_only",
                    "venue_id": "$venueid",
                    "reporting_id": "$reportingid",
                },
                "total_amount": {"$sum": "$totalamount_num"},
                "gross_amount": {"$sum": "$grossamount_num"},
                "tax_total_amount": {"$sum": "$taxtotalamount_num"},
                "before_discount_tax_total_amount": {"$sum": "$beforediscounttaxtotalamount_num"},
                "before_discount_total_amount": {"$sum": "$beforediscounttotalamount_num"},
                "transaction_count": {"$sum": 1},
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
                continue

        # venues part
        venue_id = r["_id"]["venue_id"]
        if venue_id in venue_list:
            venue_key = venue_list[venue_id].venue_key
        else:
            venues = await db.execute(
                select(DimVenues).where(
                    DimVenues.venue_external_id == (r["_id"]["venue_id"])
                )
            )

            venue = venues.scalar_one_or_none()
            if venue:
                venue_list[venue_id] = venue
                venue_key = venue.venue_key
            else:
                venue_key = None

        matched_data.append({
            "date_key": dim_date.date_key,
            "hour": r["_id"]["hour"],
            "consumer_key": consumer_key,
            "venue_key": venue_key,
            "total_amount": r["total_amount"],
            "gross_amount": r["gross_amount"],
            "tax_total_amount": r["tax_total_amount"],
            "before_discount_tax_total_amount": r["before_discount_tax_total_amount"],
            "before_discount_total_amount": r["before_discount_total_amount"],
            "transaction_count": r["transaction_count"],
        })

    if not matched_data:
        return None

    results = []
    try:
        for chunk in _chunked(matched_data, 1000):
            upsert = await _upsert_batch(
                db=db,
                model=FactConsumerSalesHourlySummary,
                data_list=chunk,
                index_elements=["date_key", "hour", "venue_key", "consumer_key"],
                exclude_from_update=["consumer_sales_hourly_summary_key"]
            )
            results.append(upsert)
            await db.commit()
    except Exception:
        await db.rollback()
        raise

    return results

async def consumer_sales_header_summary(
    db: AsyncSession,
    start_of_day: datetime,
    end_of_day: datetime
):
    pipeline = [
        {
            "$match": {
                # "transaction_source_time_local": {"$gte": to_iso_z(start_of_day), "$lt": to_iso_z(end_of_day)}
                "dateoccurred": {"$gte": to_iso_z(start_of_day), "$lt": to_iso_z(end_of_day)},
                # "$or": [
                #     {"transactionkind": "Discount"},
                #     {
                #         "transactionkind": "Sale",
                #         "$and": [
                #             {"offerids": {"$in": [None, ""]}}
                #         ]
                #     }
                # ]
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
                    "reporting_id": "$reportingid",
                    "day_part": "$daypart",
                    "pod_type": "$podtype",
                    "transaction_kind": "$transactionkind",
                    "order_take_platform": "$ordertakeplatform",
                    "sale_type": "$saletype",
                    "offer_id": "$offerids"
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
        offer_id = r["_id"]["offer_id"]
        total_amount = r["total_amount"]

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
                continue

        # venues part
        venue_id = r["_id"]["venue_id"]
        if venue_id in venue_list:
            venue_key = venue_list[venue_id].venue_key
        else:
            venues = await db.execute(
                select(DimVenues).where(
                    DimVenues.venue_external_id == (r["_id"]["venue_id"])
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
            "day_part": r["_id"]["day_part"],
            "pod_type": r["_id"]["pod_type"],
            "transaction_kind": r["_id"]["transaction_kind"],
            "order_take_platform": r["_id"]["order_take_platform"],
            "sale_type": r["_id"]["sale_type"],
            "transaction_type": _determine_transaction_type(offer_id, total_amount),
            "total_amount": total_amount,
            "gross_amount": r["gross_amount"],
            "tax_total_amount": r["tax_total_amount"],
            "before_discount_tax_total_amount": r["before_discount_tax_total_amount"],
            "before_discount_total_amount": r["before_discount_total_amount"],
        })

    if not matched_data:
        return None

    # Mongo groups by offer_id too, which is finer than this unique constraint,
    # so rows differing only by offer_id must be merged first, otherwise the
    # same batch would hit the same conflict target twice.
    index_elements = [
        "date_key", "venue_key", "consumer_key", "transaction_kind",
        "day_part", "pod_type", "order_take_platform", "sale_type", "transaction_type"
    ]
    sum_fields = [
        "total_amount", "gross_amount", "tax_total_amount",
        "before_discount_tax_total_amount", "before_discount_total_amount"
    ]

    merged = {}
    for row in matched_data:
        key = tuple(row[k] for k in index_elements)
        if key not in merged:
            merged[key] = dict(row)
            continue

        existing = merged[key]
        for field in sum_fields:
            existing[field] += row[field]

    matched_data = list(merged.values())

    results = []
    try:
        for chunk in _chunked(matched_data, 1000):
            upsert = await _upsert_batch(
                db=db,
                model=FactConsumerSalesHeaderSummary,
                data_list=chunk,
                index_elements=index_elements,
                exclude_from_update=["consumer_sales_header_summary_key"]
            )
            results.append(upsert)
            await db.commit()
    except Exception:
        await db.rollback()
        raise

    return results