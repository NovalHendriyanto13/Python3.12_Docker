from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from configs.database import get_db
from app.payloads.sync_requests.sync_request import SyncRequest
from app.controllers.sync_controller import SyncController

router = APIRouter(
    prefix= "/mongo"
)

@router.post("/sync-consumers")
async def sync_consumer_mongo(payload: SyncRequest, db: AsyncSession = Depends(get_db)):
    return await SyncController.sync_consumers_mongo(payload, db)

@router.post("/sync-venues")
async def sync_venue_mongo(payload: SyncRequest, db: AsyncSession = Depends(get_db)):
    return await SyncController.sync_venues_mongo(payload, db)