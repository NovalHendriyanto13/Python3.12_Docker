def to_double_safe(field: str) -> dict:
    """Safely convert a string field to double, defaulting to 0 on error/null."""
    return {
        "$convert": {
            "input": f"${field}",
            "to": "double",
            "onError": 0,
            "onNull": 0,
        }
    }