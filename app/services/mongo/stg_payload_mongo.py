from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.venues_model import Venues
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def transform_payload(db: AsyncSession, mongo_doc: dict):
    payload = mongo_doc.get('payload')
    
