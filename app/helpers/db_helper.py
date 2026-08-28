from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from datetime import datetime
from app.helpers.app_helper import to_date_obj
from app.models.dim_date_model import DimDate

async def _upsert_batch(
        db: AsyncSession, 
        data_list: list[dict],
        model,
        index_elements: list[str],
        exclude_from_update: list[str] = None
    ) -> int:

    if not data_list:
        return 0

    if exclude_from_update is None:
        exclude_from_update = []

    excluded_cols = set(index_elements) | set(exclude_from_update)

    stmt = insert(model).values(data_list)

    update_cols = {
        col.name: stmt.excluded[col.name]
        for col in model.__table__.columns
        if col.name not in (excluded_cols)
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=index_elements,
        set_=update_cols
    )

    await db.execute(stmt)

    return len(data_list)

async def _get_dim_date_key(db: AsyncSession, target_date: datetime):
    stmt = select(DimDate.date_key).where(DimDate.full_date == target_date)
    result = await db.execute(stmt)
    data_key = result.scalar_one_or_none()

    return data_key

async def _get_dim_key_date_list(db: AsyncSession, target_dates: list):
    try:
        date_objs = [to_date_obj(d) for d in target_dates]
        stmt = select(DimDate).where(DimDate.full_date.in_(date_objs))
        result = await db.execute(stmt)
        return result.scalars().all()
    except DBAPIError:
        raise