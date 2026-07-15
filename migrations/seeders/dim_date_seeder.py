import argparse
import asyncio
from datetime import date, timedelta

from sqlalchemy import select

from configs.database import AsyncSessionLocal
from app.models.dim_date_model import DimDate


def generate_dates(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def get_quarter(month: int) -> int:
    return (month - 1) // 3 + 1


async def seed_date_range(start_date: date, end_date: date) -> int:
    """Seed dim_date untuk rentang [start_date, end_date] inklusif.
    Idempotent: tanggal yang sudah ada tidak akan di-duplikasi.
    Return jumlah baris baru yang berhasil di-insert.
    """
    async with AsyncSessionLocal() as session:
        try:
            result = await session.execute(
                select(DimDate.full_date).where(
                    DimDate.full_date.between(start_date, end_date)
                )
            )
            existing_dates = {row[0] for row in result.all()}

            new_rows = [
                DimDate(
                    full_date=d,
                    day=d.day,
                    month=d.month,
                    year=d.year,
                    quarter=get_quarter(d.month),
                )
                for d in generate_dates(start_date, end_date)
                if d not in existing_dates
            ]

            if new_rows:
                session.add_all(new_rows)
                await session.commit()
                print(f"[seed_dim_date] Seeded {len(new_rows)} tanggal baru ({start_date} s/d {end_date})")
            else:
                print(f"[seed_dim_date] Tidak ada tanggal baru, sudah up to date ({start_date} s/d {end_date})")

            return len(new_rows)
        except Exception as e:
            await session.rollback()
            print(f"[seed_dim_date] Gagal seeding: {e}")
            raise


async def seed_last_n_years(years_back: int = 2) -> int:
    today = date.today()
    start_date = date(today.year - years_back, 1, 1)
    end_date = date(today.year, 12, 31)
    return await seed_date_range(start_date, end_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed dim_date N tahun ke belakang")
    parser.add_argument(
        "--years-back",
        type=int,
        default=2,
        help="Jumlah tahun ke belakang yang di-seed (default: 2)",
    )
    args = parser.parse_args()

    asyncio.run(seed_last_n_years(years_back=args.years_back))