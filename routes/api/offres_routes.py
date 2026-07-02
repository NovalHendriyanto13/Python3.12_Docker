from fastapi import APIRouter, Depends
from app.payloads.offres.etl_offers_payload import EtlOffersPayload

router = APIRouter(
    prefix= "/offers"
)

# @router.post("/sync")
# async def etlSync()