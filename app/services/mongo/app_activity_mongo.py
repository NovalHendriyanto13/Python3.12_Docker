from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from app.models.fact_app_activity_summary import FactAppActivitySummary
from app.helpers.app_helper import to_int, get_today_range, to_iso_z, _chunked
from app.helpers.db_helper import _upsert_batch, _get_dim_key_date_list
from configs.mongo import mongo_conn

async def upsert_app_activity(db: AsyncSession, mongo_doc: dict, is_init: bool = False):
    start_datetime, end_datetime = get_today_range()
    try:
        await app_activity_summary(db, start_datetime, end_datetime)
        
        await db.commit()
    except Exception:
        await db.rollback()
        raise

# mcd_consumer_activities stores pre-aggregated hourly counters per consumer
# (downloadcount, deviceregistrationcount, registrationcount, ...) rather than
# one document per event, so activities are summed straight off those fields.
# The fact table doesn't keep consumer identity, only how many distinct
# consumers triggered each activity, so counting happens in two group stages:
# per (date, consumer) first, then collapsed to per date across consumers.
ACTIVITY_FIELD_MAP = {
    "download": "Download",
    "new_device": "New Device",
    "registration": "Registration",
}

async def app_activity_summary(
    db: AsyncSession,
    start_of_day: datetime,
    end_of_day: datetime
):
    pipeline = [
        {
            "$match": {
                "sourceactivitytimeutc": {"$gte": to_iso_z(start_of_day), "$lt": to_iso_z(end_of_day)}
            }
        },
        {
            "$addFields": {
                "date_only": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": {"$dateFromString": {"dateString": "$sourceactivitytimeutc"}}
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
                "download": {"$sum": {"$toInt": "$downloadcount"}},
                "new_device": {"$sum": {"$toInt": "$deviceregistrationcount"}},
                "registration": {"$sum": {"$toInt": "$registrationcount"}},
            }
        },
        {
            "$group": {
                "_id": "$_id.date",
                "total_download": {"$sum": "$download"},
                "consumers_download": {"$sum": {"$cond": [{"$gt": ["$download", 0]}, 1, 0]}},
                "total_new_device": {"$sum": "$new_device"},
                "consumers_new_device": {"$sum": {"$cond": [{"$gt": ["$new_device", 0]}, 1, 0]}},
                "total_registration": {"$sum": "$registration"},
                "consumers_registration": {"$sum": {"$cond": [{"$gt": ["$registration", 0]}, 1, 0]}},
            }
        }
    ]
    cursor = mongo_conn["mcd_consumer_activities"].aggregate(pipeline)
    agg_result = await cursor.to_list()

    if not agg_result:
        return None

    unique_dates = sorted(set(r["_id"] for r in agg_result))

    # get date key
    date_keys = await _get_dim_key_date_list(db, unique_dates)

    dim_dates_map = {
        d.full_date.strftime("%Y-%m-%d") if hasattr(d.full_date, "strftime") else d.full_date: d
        for d in date_keys
    }

    matched_data = []
    for r in agg_result:
        date_only = r["_id"]
        dim_date = dim_dates_map.get(date_only)

        if dim_date is None:
            print(f"WARNING: no dim_date found for {date_only}")
            continue

        for field, activity_name in ACTIVITY_FIELD_MAP.items():
            total_count = to_int(r.get(f"total_{field}")) or 0
            consumer_count = to_int(r.get(f"consumers_{field}")) or 0
            if total_count <= 0 and consumer_count <= 0:
                continue

            matched_data.append({
                "date_key": dim_date.date_key,  # adjust field name to match your DimDates model
                "activity": activity_name,
                "total_count": total_count,
                "consumer_count": consumer_count
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
                model=FactAppActivitySummary,
                data_list=chunk,
                index_elements=["date_key", "activity"],
                exclude_from_update=["app_activity_key"]
            )
            results.append(upsert)
            await db.commit()
    except Exception:
        await db.rollback()
        raise

    return results