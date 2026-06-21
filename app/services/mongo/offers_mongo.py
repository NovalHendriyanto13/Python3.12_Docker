from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.offers_model import Offers

async def upsert_offers(db: AsyncSession, mongo_doc: dict):
    stmt = insert(Offers).values(
        id=str(mongo_doc["id"]),
        campaignid=mongo_doc["campaignid"],
        title=mongo_doc["title"],
        description=mongo_doc["description"],
        category=mongo_doc["category"],
        codetype=mongo_doc["codetype"],
        status=mongo_doc["status"],
        isreward=mongo_doc["isreward"],
        whenstarts=mongo_doc["whenstarts"],
        whenexpires=mongo_doc["whenexpires"],
        whenlastupdated=mongo_doc["whenlastupdated"],
        redemptiontext=mongo_doc["redemptiontext"],
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "campaignid": mongo_doc["campaignid"],
            "title": mongo_doc["title"],
            "description": mongo_doc["description"],
            "category": mongo_doc["category"],
            "codetype": mongo_doc["codetype"],
            "status": mongo_doc["status"],
            "isreward": mongo_doc["isreward"],
            "whenstarts": mongo_doc["whenstarts"],
            "whenexpires": mongo_doc["whenexpires"],
            "whenlastupdated": mongo_doc["whenlastupdated"],
            "redemptiontext": mongo_doc["redemptiontext"],
        }
    )
    await db.execute(stmt)
    await db.commit()
