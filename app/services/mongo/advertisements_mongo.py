from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.advertisements_model import Advertisements

async def upsert_advertisements(db: AsyncSession, mongo_doc: dict):
    stmt = insert(Advertisements).values(

        id = mongo_doc["id"],  
        campaignid = mongo_doc["campaignid"],
        title = mongo_doc["title"],
        description = mongo_doc["description"],
        startdate = mongo_doc["startdate"],
        enddate = mongo_doc["enddate"],
        status = mongo_doc["status"],

    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "id": mongo_doc["id"],
            "campaignid": mongo_doc["campaignid"],
            "title": mongo_doc["title"],
            "description": mongo_doc["description"],
            "startdate": mongo_doc["startdate"],
            "enddate": mongo_doc["enddate"],
            "status": mongo_doc["status"],
        }
    )
    await db.execute(stmt)
    await db.commit()
