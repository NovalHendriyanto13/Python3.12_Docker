import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

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

