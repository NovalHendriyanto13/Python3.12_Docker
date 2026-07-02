from fastapi.concurrency import run_in_threadpool
from configs.databricks import databricks_fetch_data

async def get_data():
    query = "SELECT * FROM samples.bakehouse.sales_customers LIMIT 10"

    rows = await run_in_threadpool(
        databricks_fetch_data,
        "sales_customers"
    )

    if not rows:
        return {"status": "no data"}

    return {
        "status": "success",
        "inserted": rows
    }