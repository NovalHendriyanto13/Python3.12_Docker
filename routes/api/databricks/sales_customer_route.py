from fastapi import APIRouter
from app.controllers.databricks.sales_customer_controller import get_data

router = APIRouter(
    prefix="/databricks/sales_customer"
)

@router.get("/")
async def get_customer():
    return await get_data()