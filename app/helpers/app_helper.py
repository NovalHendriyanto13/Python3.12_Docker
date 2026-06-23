import uuid
from datetime import datetime
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
    if isinstance(val, datetime):
        return val
    try:
        clean_val = val.replace("Z", "+00:00") if isinstance(val, str) else val
        return datetime.fromisoformat(clean_val)
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