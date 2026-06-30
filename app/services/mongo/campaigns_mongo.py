from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from app.models.campaigns_model import Campaigns
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool
from app.services.staging.data_changes_staging import StagingDataChangesService

async def upsert_campaigns(db: AsyncSession, mongo_doc: dict):
    campaign_id = to_int(mongo_doc.get('campaign_id'))
    
    existing = await db.execute(
        select(Campaigns).where(
            Campaigns.campaign_id == campaign_id
        )
    )

    existing_record = existing.scalar_one_or_none()

    new_values = dict(
        campaign_id=campaign_id,
        market=mongo_doc.get('market'),
        title=mongo_doc.get('title'),
        campaign_status=to_int(mongo_doc.get('campaigns_status')),
        status=mongo_doc.get('status'),
        creation_date=to_datetime(mongo_doc.get('creation_date')),
        modified_date=to_datetime(mongo_doc.get('modified_date'))
    )

    stmt=insert(Campaigns).values(**new_values).on_conflict_do_update(
        index_elements=["campaign_id"],
        set_=new_values
    )
    await db.execute(stmt)

    if existing_record is not None:
        await StagingDataChangesService(
            db=db,
            module_name="mcd_campaigns",
            module_id=campaign_id,
            old_data=existing_record,
            new_data=new_values
        )

    await db.commit()
