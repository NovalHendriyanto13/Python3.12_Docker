from sqlalchemy.ext.asyncio import AsyncSession
from app.helpers.app_helper import _serialize
from app.models.stg_data_changes_model import StgDataChanges

async def StagingDataChangesService(
    db:AsyncSession,
    module_name: str,
    module_id: str,
    old_data: object,
    new_data: dict
):
    insert_changes = StgDataChanges(
        module_name= module_name,
        module_id= module_id,
        old_data= _serialize(old_data),
        new_data= _serialize(new_data)
    )

    db.add(insert_changes)

