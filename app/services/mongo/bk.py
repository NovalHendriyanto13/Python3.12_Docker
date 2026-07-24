"""
Agregasi fact_sales_summary dan fact_visits_summary dari sales_headers.

Sumber data: sales_headers (kolom transaction_source_time_local, market,
venue_external_id, total_amount, dst).

Grouping:
- fact_sales_summary: per bulan saja (year + month), tanpa breakdown market/venue.
- fact_visits_summary: per full_date (tanggal dari transaction_source_time_local)
  + market + venue_external_id.

total_visit dihitung dari jumlah jam unik (menit & detik diabaikan) yang
punya transaksi pada kombinasi tanggal + market + venue tersebut.
Contoh: venue A, market ID, tanggal 2026-07-09, ada transaksi jam 10:05
dan 10:47 -> dihitung 1 visit (sama-sama jam 10). Kalau ada transaksi lagi
jam 11:02 -> jadi 2 visit.
"""

from datetime import date
from typing import Optional

from sqlalchemy import select, func, cast, Date as SA_Date
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from models.sales_headers import SalesHeaders
from models.fact_sales_summary import FactSalesSummary
from models.fact_visits_summary import FactVisitsSummary


def _full_date_expr():
    return cast(SalesHeaders.transaction_source_time_local, SA_Date)


async def aggregate_sales_summary(db: AsyncSession, target_date: Optional[date] = None) -> int:
    """
    Agregasi total_amount, tax_total_amount, dst dari sales_headers,
    dikelompokkan per bulan (year + month) -- tanpa breakdown market/venue,
    lalu upsert ke fact_sales_summary.

    target_date: kalau diisi, hanya agregasi ulang untuk bulan yang memuat
    tanggal tersebut (dipakai untuk re-run bulan berjalan). Kalau None,
    agregasi seluruh data di sales_headers.
    """
    year_expr = func.extract("year", SalesHeaders.transaction_source_time_local)
    month_expr = func.extract("month", SalesHeaders.transaction_source_time_local)

    query = select(
        year_expr.label("year"),
        month_expr.label("month"),
        func.sum(SalesHeaders.total_amount).label("total_amount"),
        func.sum(SalesHeaders.tax_total_amount).label("tax_total_amount"),
        func.sum(SalesHeaders.gross_amount).label("gross_amount"),
        func.sum(SalesHeaders.before_discount_tax_total_amount).label("before_discount_tax_total_amount"),
        func.sum(SalesHeaders.before_discount_total_amount).label("before_discount_total_amount"),
    ).group_by(year_expr, month_expr)

    if target_date is not None:
        query = query.where(
            year_expr == target_date.year,
            month_expr == target_date.month,
        )

    result = await db.execute(query)
    rows = result.all()

    for row in rows:
        year = int(row.year)
        month = int(row.month)
        values = dict(
            full_date=date(year, month, 1),
            date=1,
            month=month,
            year=year,
            total_amount=row.total_amount,
            tax_total_amount=row.tax_total_amount,
            gross_amount=row.gross_amount,
            before_discount_tax_total_amount=row.before_discount_tax_total_amount,
            before_discount_total_amount=row.before_discount_total_amount,
        )

        stmt = insert(FactSalesSummary).values(**values).on_conflict_do_update(
            index_elements=["year", "month"],
            set_=values,
        )
        await db.execute(stmt)

    await db.commit()
    print(f"[fact_sales_summary] Upserted {len(rows)} baris")
    return len(rows)


async def aggregate_visits_summary(db: AsyncSession, target_date: Optional[date] = None) -> int:
    """
    Hitung total_visit per full_date + market + venue, dengan 1 visit =
    1 kombinasi unik (venue, market, tanggal, jam) -- menit & detik diabaikan.
    Lalu upsert ke fact_visits_summary.
    """
    full_date_expr = _full_date_expr()
    hour_bucket_expr = func.date_trunc("hour", SalesHeaders.transaction_source_time_local)

    query = select(
        full_date_expr.label("full_date"),
        SalesHeaders.market,
        SalesHeaders.venue_external_id,
        func.count(func.distinct(hour_bucket_expr)).label("total_visit"),
    ).group_by(full_date_expr, SalesHeaders.market, SalesHeaders.venue_external_id)

    if target_date is not None:
        query = query.where(full_date_expr == target_date)

    result = await db.execute(query)
    rows = result.all()

    for row in rows:
        values = dict(
            full_date=row.full_date,
            date=row.full_date.day,
            month=row.full_date.month,
            year=row.full_date.year,
            market=row.market,
            venue=row.venue_external_id,
            total_visit=row.total_visit,
        )

        stmt = insert(FactVisitsSummary).values(**values).on_conflict_do_update(
            index_elements=["full_date", "market", "venue"],
            set_=values,
        )
        await db.execute(stmt)

    await db.commit()
    print(f"[fact_visits_summary] Upserted {len(rows)} baris")
    return len(rows)


async def aggregate_all_summary(db: AsyncSession, target_date: Optional[date] = None) -> dict:
    """Helper untuk jalankan keduanya sekaligus, misal dipanggil dari cron harian."""
    sales_count = await aggregate_sales_summary(db, target_date=target_date)
    visits_count = await aggregate_visits_summary(db, target_date=target_date)
    return {"sales_rows": sales_count, "visits_rows": visits_count}