from fastapi import APIRouter
import routes.api.databricks as databrick_routes

router = APIRouter(
    prefix="/api"
)

router.include_router(databrick_routes.sales_customer_route.router)