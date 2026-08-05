"""
Cron seeder (async): dijalankan otomatis setiap tanggal 1 Januari untuk
mengisi dim_date satu tahun penuh untuk tahun yang baru saja dimulai.

Jadwalkan via crontab, contoh (jalan tiap 1 Jan jam 00:05):
    5 0 1 1 * /path/to/venv/bin/python /path/to/project/seeders/seed_new_year_dim_date.py >> /var/log/dim_date_seeder.log 2>&1
"""

import asyncio
from datetime import date

from migrations.seeders.seed_dim_date import seed_date_range


async def seed_current_year() -> int:
    """Seed 1 tahun penuh untuk tahun berjalan.
    Didesain untuk dijalankan tepat di tanggal 1 Januari, sehingga
    date.today().year sudah merujuk ke tahun yang baru mulai.
    """
    target_year = date.today().year
    start_date = date(target_year, 1, 1)
    end_date = date(target_year, 12, 31)
    return await seed_date_range(start_date, end_date)

if __name__ == "__main__":
    asyncio.run(seed_current_year())