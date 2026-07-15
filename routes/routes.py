from fastapi import APIRouter
import routes.api.mongo as mongo_routes 

router = APIRouter(
    prefix="/api"
)

router.include_router(mongo_routes.sync_routes.router)