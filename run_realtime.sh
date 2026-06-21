#!/bin/bash
# Jalankan Prefect Server dan FastAPI service bersamaan dalam satu script.

# Tentukan direktori kerja server
PROJECT_DIR="/var/www/html/works/mcd_cdp/processor_python"

if [ -d "$PROJECT_DIR" ]; then
    cd "$PROJECT_DIR"
else
    # Jika dijalankan secara lokal di folder workspace saat ini
    cd "$(dirname "$0")"
fi

# Hentikan semua proses background jika script ini dimatikan (Ctrl+C atau service stop)
trap 'echo "🛑 Menghentikan semua service..."; kill $(jobs -p) 2>/dev/null' EXIT

echo "🚀 1. Memulai Prefect Server di background..."
./venv/bin/prefect server start --host 0.0.0.0 &

# Tunggu 5 detik agar Prefect Server selesai inisialisasi
echo "⏳ Menunggu Prefect Server siap..."
sleep 5

echo "🚀 2. Memulai FastAPI Realtime Sync..."
./venv/bin/python main.py
