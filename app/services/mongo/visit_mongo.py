from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime
from app.models.dim_venues_model import DimVenues
from app.models.dim_date_model import DimDate
from app.models.dim_consumer_model import DimConsumers
from app.models.fact_consumer_visit_summary import FactConsumerVisitSummary
from app.helpers.app_helper import to_int, get_today_range, to_iso_z, _chunked, to_uuid
from app.helpers.db_helper import _upsert_batch, _get_dim_key_date_list
from configs.mongo import mongo_conn

async def upsert_visit(db: AsyncSession, mongo_doc: dict, is_init: bool = False):
    start_datetime, end_datetime = get_today_range()
    try:
        await visit_summary(db, start_datetime, end_datetime)
        
        await db.commit()
    except Exception:
        await db.rollback()
        raise

async def visit_summary(
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
                    # "market": "$market"
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
            "total_visit": to_int(r["total_visit"]),
            # "market": r["_id"]["market"]
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
                model=FactConsumerVisitSummary,
                data_list=chunk,
                index_elements=["date_key", "venue_key", "consumer_key"],
                exclude_from_update=["consumer_visit_summary_key"]
            )
            results.append(upsert)
            await db.commit()
    except Exception:
        await db.rollback()
        raise 
    
    return results