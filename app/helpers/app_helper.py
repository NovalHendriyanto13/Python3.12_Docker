import uuid
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
from fastapi import Query, HTTPException
from typing import Optional, Union

def to_uuid(val):
    if not val or val == "":
        return None
    try:
        return uuid.UUID(val)
    except (ValueError, TypeError):
        return None

def to_datetime(val):
    if not val or val == "":
        return None

    if isinstance(val, date):
        return val if not isinstance(val, datetime) else val.date()

    try:
        clean_val = val.replace("Z", "+00:00")
        return datetime.fromisoformat(clean_val).date()
    except (ValueError, TypeError):
        try:
            return date.fromisoformat(val)
        except (ValueError, TypeError):
            return None

def to_int(val):
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None

def to_decimal(val):
    if val is None or val == "":
        return None
    try:
        return Decimal(val)
    except (ValueError, TypeError):
        return None

_UUID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, "mcd_cdp.processor_python")

def to_deterministic_uuid(*parts) -> uuid.UUID:
    """Build a stable UUID from natural-key parts, so FK keys can be derived
    without a DB round-trip and stay identical across re-runs."""
    key = "|".join("" if p is None else str(p) for p in parts)
    return uuid.uuid5(_UUID_NAMESPACE, key)

def to_bool(val):
    if val is None or val == "":
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("true", "1", "t", "y", "yes")
    return bool(val)

def _serialize(value):
    """Convert any value to a JSON-safe primitive."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (list, tuple)):
        return [_serialize(v) for v in value]
    if isinstance(value, dict):
        return {k: _serialize(v) for k, v in value.items()}
    # SQLAlchemy model object — convert all columns to dict
    if hasattr(value, '__table__'):
        return {
            col.name: _serialize(getattr(value, col.name))
            for col in value.__table__.columns
        }
    return str(value)

def get_today_range() -> tuple[datetime, datetime]:
    now = datetime.now()
    start_of_day = datetime(now.year, now.month, now.day, 0, 0, 0, 0)
    end_of_day = start_of_day + timedelta(days=1) - timedelta(microseconds=1)
    return start_of_day, end_of_day

def get_date_range(
    start_date: Optional[Union[str, date]],
    end_date: Optional[Union[str, date]],
) -> tuple[datetime, datetime]:

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    if start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail="start_date must be before or equal to end_date",
        )

    start_dt = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0, 0)
    end_dt = datetime(end_date.year, end_date.month, end_date.day) + timedelta(days=1) - timedelta(microseconds=1)

    return start_dt, end_dt

def chunk_month_range(
    start_date: Union[str, date],
    end_date: Union[str, date],
):
    """Yield (start_dt, end_dt) per calendar month so a wide init range
    doesn't get aggregated against Mongo in a single heavy query."""
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    current = date(start_date.year, start_date.month, 1)
    while current <= end_date:
        if current.month == 12:
            next_month = date(current.year + 1, 1, 1)
        else:
            next_month = date(current.year, current.month + 1, 1)

        chunk_start = max(current, start_date)
        chunk_end = min(next_month - timedelta(days=1), end_date)

        yield get_date_range(chunk_start, chunk_end)

        current = next_month

def to_utc_datetime(dt: datetime) -> datetime:
    """Pastikan datetime dalam UTC dan timezone-aware untuk query MongoDB."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt

def to_iso_z(value: Union[str, datetime], fmt: str = "%Y-%m-%d") -> str:
    if isinstance(value, str):
        dt = datetime.strptime(value, fmt)
        dt = dt.replace(tzinfo=timezone.utc)
    elif isinstance(value, datetime):
        dt = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    else:
        raise TypeError(f"Expected str or datetime, got {type(value).__name__}")

    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

def to_date_obj(d: Union[str, date, datetime]) -> date:
    """Normalize a string, date, or datetime into a plain date object."""
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    if isinstance(d, str):
        return datetime.strptime(d, "%Y-%m-%d").date()
    raise TypeError(f"Cannot convert {type(d).__name__} to date")

def _chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]