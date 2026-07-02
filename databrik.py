import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from prefect import flow
from app.tasks.sample_task import databricks_to_mongo_sync

@flow(name="master-etl", log_prints=True)
async def master_flow():
    await databricks_to_mongo_sync()

if __name__ == "__main__":
    asyncio.run(master_flow())