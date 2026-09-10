#!/bin/bash
# Jalankan Prefect Server dan FastAPI service bersamaan dalam satu script.

# Tentukan direktori kerja server: ikut lokasi file script ini berada,
# bukan $PWD (yang berubah-ubah tergantung dari mana script dipanggil).
# Dengan systemd, WorkingDirectory= sudah mengarah ke sini juga, jadi hasilnya sama.
PROJECT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
cd "$PROJECT_DIR"

# Hentikan semua proses background jika script ini dimatikan (Ctrl+C atau service stop)
trap 'echo "🛑 Menghentikan semua service..."; kill $(jobs -p) 2>/dev/null' EXIT

echo "🚀 1. Memulai Prefect Server di background..."
./venv/bin/prefect server start --host 0.0.0.0 &

# Tunggu 5 detik agar Prefect Server selesai inisialisasi
echo "⏳ Menunggu Prefect Server siap..."
sleep 5

echo "🚀 2. Memulai FastAPI Realtime Sync..."
./venv/bin/python main.py
