from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

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
    await db.commit()

    return len(data_list)
