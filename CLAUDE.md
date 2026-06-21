# CLAUDE.md

## Project Overview
MCD CDP Python Processor handles the ingestion and synchronization of data from MongoDB to PostgreSQL. It has two modes of operation:
1. **Real-Time Sync**: FastAPI service that listens to MongoDB Change Streams (`$changeStream`) and upserts new/updated data instantly.
2. **Bulk ETL**: Prefect-based flows for migrating large amounts of records (e.g., 30 million records) in bulk.

---

## Tech Stack
- **Language**: Python 3.12 (inside virtualenv `venv`)
- **Web / Service**: FastAPI & Uvicorn
- **Databases**: 
  - MongoDB (configured as a replica set `rs0` for Change Streams support)
  - PostgreSQL (target database)
- **Database Libraries**:
  - Motor (async MongoDB client)
  - SQLAlchemy with `asyncpg` driver (async PostgreSQL client)
- **Orchestration**: Prefect (for Bulk ETL management)

---

## Codebase Architecture
- `main.py` - FastAPI entry point. Launches the real-time listener tasks in its lifespan.
- `run_etl.py` - Script to execute the bulk ETL flow (runs consumer and offers flows sequentially).
- `run_realtime.sh` - Bash script that runs both the Prefect Server and the FastAPI application together.
- `etl/` - Core bulk ETL scripts:
  - `consumers_etl.py` - Bulk consumer sync flow (Chunked COPY staging + Upsert).
  - `offers_etl.py` - Bulk offers sync flow (Chunked COPY staging + Upsert).
- `app/`
  - `listeners/mongo_listener.py` - Change Stream listener (`watch_mongo`) that tracks insertions/updates.
  - `services/mongo/` - Contains the upsert implementation (`consumers_mongo.py`, `offers_mongo.py`).
  - `models/` - SQLAlchemy models defining target PostgreSQL schemas.
- `configs/` - App configuration and database session pool settings.

---

## Commands & Workflows

### 1. Run Real-Time Service & Prefect Server
To run both services concurrently with automatic background process cleanup:
```bash
chmod +x run_realtime.sh
./run_realtime.sh
```

### 2. Run Bulk ETL Manually (One-time Sync)
Ensure the Prefect server is running first, then run:
```bash
./venv/bin/python run_etl.py
```

### 3. Check Python Syntax Compilation
Verify all python files compile successfully without syntax errors:
```bash
./venv/bin/python -m py_compile etl/consumers_etl.py etl/offers_etl.py
```

---

## Systemd Deployment (Ubuntu)
To keep the real-time processor and Prefect server running in the background ("ON Terus"):

- **Service File**: `/etc/systemd/system/mcd_processor.service` (copied from `mcd_processor.service` in root)
- **Commands**:
  ```bash
  # Reload daemon after changes
  sudo systemctl daemon-reload

  # Manage service
  sudo systemctl start mcd_processor
  sudo systemctl stop mcd_processor
  sudo systemctl restart mcd_processor
  sudo systemctl enable mcd_processor # Run on boot
  sudo systemctl status mcd_processor

  # Watch real-time logs
  sudo journalctl -u mcd_processor -f
  ```

---

## Coding Rules & Best Practices
1. **Strict Type Casting**: Because the PostgreSQL schema uses `character varying` (String) for most columns (e.g. IDs, integers, booleans), but MongoDB stores them as raw numbers/booleans, **always explicitly cast MongoDB fields to string** (e.g. `str(doc.get("isreward")).lower()`) before writing to Postgres to prevent datatype mismatch errors.
2. **Chunking in Bulk ETL**: Do not execute 30M rows in a single SQL transaction. Always split bulk loads into chunks of **1,000,000 records** and yield control using `await asyncio.sleep(0.1)` between chunks to allow the Prefect heartbeat to report successfully.
3. **No Raw SQL string execution**: Always wrap raw SQL strings in SQLAlchemy's `text()` function before calling `conn.execute(text("SQL"))`.
4. **Error logs visibility**: Never swallow exceptions silently in the listeners; print traceback using `traceback.print_exc()` to make them visible in `journalctl`.
