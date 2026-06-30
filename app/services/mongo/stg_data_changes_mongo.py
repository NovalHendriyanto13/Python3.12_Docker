from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.stg_data_changes_model import StgDataChanges
from app.helpers.app_helper import to_uuid, to_datetime, to_int, to_decimal, to_bool

async def upsert_stg_data_changes(db: AsyncSession, mongo_doc: dict):
    stmt=insert(StgDataChanges).values(
        module_name=mongo_doc.get('module_name'),
        module_id=mongo_doc.get('module_id'),
        old_data=mongo_doc.get('old_data'),
        new_data=mongo_doc.get('new_data')
    ).on_conflict_do_update(
        index_elements=["module_id", "module_name"],
        set_={
            "module_name": mongo_doc.get('module_name'),
            "module_id": mongo_doc.get('module_id'),
            "old_data": mongo_doc.get('old_data'),
            "new_data": mongo_doc.get('new_data')
        }
    )
    await db.execute(stmt)
    await db.commit()
