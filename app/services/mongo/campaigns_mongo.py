from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.campaigns_model import Campaigns
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_campaigns(db: AsyncSession, mongo_doc: dict):
    stmt=insert(Campaigns).values(
        campaign_id=to_int(mongo_doc.get('campaign_id')),
        market=mongo_doc.get('market'),
        title=mongo_doc.get('title'),
        campaigns_status=to_int(mongo_doc.get('campaigns_status')),
        status=mongo_doc.get('status'),
        creation_date=to_datetime(mongo_doc.get('creation_date')),
        modified_date=to_datetime(mongo_doc.get('modified_date')),
    ).on_conflict_do_update(
        index_elements=["id"],
        set_={
            "campaign_id": to_int(mongo_doc.get('campaign_id')),
            "market": mongo_doc.get('market'),
            "title": mongo_doc.get('title'),
            "campaigns_status": to_int(mongo_doc.get('campaigns_status')),
            "status": mongo_doc.get('status'),
            "creation_date": to_datetime(mongo_doc.get('creation_date')),
            "modified_date": to_datetime(mongo_doc.get('modified_date')),
        }
    )
    await db.execute(stmt)
    await db.commit()
