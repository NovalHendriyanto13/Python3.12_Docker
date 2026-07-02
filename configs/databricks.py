import re
from configs.app_config import databricks_server_hostname, databricks_http_path, databricks_token, databricks_catalog, databricks_schema
from databricks import sql

ALLOWED_OPERATORS = {"=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", "LIKE", "IS NULL", "IS NOT NULL"}

def databricks_connection():
    return sql.connect(
        server_hostname=databricks_server_hostname,
        http_path=databricks_http_path,
        access_token=databricks_token,
    )

def _validate_identifier(name: str) -> str:
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        raise ValueError(f"Invalid column/identifier name: {name}")
    return name

def _build_condition(column: str, condition) -> tuple[str, list]:
    """
    Bangun satu kondisi WHERE dari value polos atau dict {op, value}.
    Return: (sql_fragment, params_list)
    """
    col = _validate_identifier(column)

    # bentuk sederhana: {"state": "CA"} -> state = %s
    if not isinstance(condition, dict):
        return f"{col} = %s", [condition]

    op = condition.get("op", "=").upper()
    value = condition.get("value")

    if op not in ALLOWED_OPERATORS:
        raise ValueError(f"Operator not allowed: {op}")

    # khusus keyword "now" -> pakai fungsi SQL current_timestamp(), bukan parameter
    if isinstance(value, str) and value.lower() == "now":
        return f"{col} {op} current_timestamp()", []

    if op in ("IS NULL", "IS NOT NULL"):
        return f"{col} {op}", []

    if op in ("IN", "NOT IN"):
        if not isinstance(value, (list, tuple)):
            raise ValueError(f"Value for {op} must be a list")
        placeholders = ", ".join(["%s"] * len(value))
        return f"{col} {op} ({placeholders})", list(value)

    return f"{col} {op} %s", [value]


def databricks_fetch_data(
    table: str,
    columns: list[str] = None,
    criteria: dict = None,
    limit: int = None,
):
    # --- SELECT clause ---
    if columns:
        validated_columns = [_validate_identifier(c) for c in columns]
        select_clause = ", ".join(validated_columns)
    else:
        select_clause = "*"

    # --- WHERE clause ---
    where_clause = ""
    params = []
    if criteria:
        conditions = []
        for col, condition in criteria.items():
            fragment, frag_params = _build_condition(col, condition)
            conditions.append(fragment)
            params.extend(frag_params)
        where_clause = "WHERE " + " AND ".join(conditions)

    # --- LIMIT clause ---
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    tablename = f"{databricks_catalog}.{databricks_schema}.{table}"

    query = f"SELECT {select_clause} FROM {tablename} {where_clause} {limit_clause}".strip()

    with databricks_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            result_columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(result_columns, row)) for row in rows]

