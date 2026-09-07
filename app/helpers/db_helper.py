from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func, cast, Integer
from sqlalchemy.exc import DBAPIError
from datetime import datetime
from app.helpers.app_helper import to_date_obj, to_decimal
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

async def _get_max_numeric_id(db: AsyncSession, model, column_name: str, width: int = 6) -> int:
    """Highest existing value of a zero-padded numeric string column
    (e.g. a 6-digit sequential key), ignoring rows that don't match that
    width so real/non-numeric ids never break the cast."""
    column = getattr(model, column_name)
    stmt = select(func.max(cast(column, Integer))).where(column.op("~")(f"^\\d{{{width}}}$"))
    result = await db.execute(stmt)
    return result.scalar_one_or_none() or 0

def _determine_transaction_type(offer_id, total_amount) -> str:
    """REWARD: total_amount is exactly 1, regardless of offer_id.
    INCENTIVISED: offer_id is present and total_amount > 1.
    UNINCENTIVISED: everything else."""
    amount = to_decimal(total_amount)

    if amount == to_decimal('1'):
        return "REWARD"

    if offer_id and amount is not None and amount > to_decimal('1'):
        return "INCENTIVISED"

    return "UNINCENTIVISED"

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
