# MCD CDP Python Processor

The **MCD CDP Python Processor** is a high-performance ingestion and synchronization system designed to migrate and sync data from **MongoDB** to **PostgreSQL**. 

It supports two modes of operation:
1. **Real-Time Sync**: A FastAPI service that listens to MongoDB Change Streams (`$changeStream`) to capture modifications and upsert them to PostgreSQL instantly.
2. **Bulk ETL**: A Prefect-orchestrated framework for bulk migrating large datasets (e.g., 30+ million records) efficiently using chunked COPY staging and upserts.

---

## 🛠️ Tech Stack

*   **Language**: Python 3.12 (managed via virtual environment `venv`)
*   **Web Framework**: FastAPI & Uvicorn (real-time service)
*   **Orchestration**: Prefect (bulk ETL orchestration)
*   **Databases**:
    *   **MongoDB** (configured as a replica set `rs0` to support Change Streams)
    *   **PostgreSQL** (target relational database)
*   **Database Libraries**:
    *   `Motor` (asynchronous MongoDB client)
    *   `SQLAlchemy` with `asyncpg` (asynchronous PostgreSQL client and ORM)

---

## 📂 Codebase Architecture

```
├── app/                       # Core application logic
│   ├── controllers/           # Route controllers and handlers
│   ├── listeners/             # MongoDB Change Stream listeners (e.g., watch_mongo)
│   ├── models/                # SQLAlchemy database models for PostgreSQL
│   ├── payloads/              # Request/response schemas and validation models
│   ├── services/              # Core business and upsert logic (e.g., services/mongo/)
│   └── tasks/                 # Background task management
├── configs/                   # Configuration management
│   ├── app_config.py          # App environment and variables
│   └── database.py            # Async engine and session pool setups
├── etl/                       # Bulk ETL Flows
│   ├── advertisements_etl.py  # Bulk ad sync flow
│   ├── consumers_etl.py       # Bulk consumer sync flow (Chunked COPY + Upsert)
│   ├── loyalty_points_card_transactions_etl.py  # Bulk transaction sync flow
│   └── offers_etl.py          # Bulk offers sync flow
├── exceptions/                # Exception handling utilities
│   └── custom_http_exception.py
├── routes/                    # API Route endpoints
├── .env                       # Environment variables config
├── CLAUDE.md                  # Development cheatsheet and coding instructions
├── main.py                    # FastAPI entrypoint (starts real-time sync listeners)
├── mcd_processor.service      # Systemd service configuration template
├── run_etl.py                 # Runner script for Bulk ETL flows
├── run_realtime.sh            # Service manager shell script (runs Prefect + FastAPI)
└── README.md                  # Project documentation (this file)
```

---

## ⚙️ Environment Configuration

Create a `.env` file in the root directory based on the following template:

```env
APP_ENV=development
APP_NAME="MCD CDP"
APP_PORT=4000

MONGO_DB=mcd_cdp
MONGO_URI=mongodb://localhost:27017/mcd_cdp
DATABASE_URL=postgresql+asyncpg://postgres:password@localhost:5432/mcd_cdp_db

# Set Prefect server API URL if running Prefect Server on a remote/specific address
PREFECT_API_URL=
```

> [!NOTE]
> Ensure MongoDB is configured as a replica set (e.g., `rs0`). Change Streams do not work on standalone MongoDB instances.

---

## 🚀 Running the Project

### 1. Setup Environment
Ensure Python 3.12 is installed, create a virtualenv, and install dependencies:
```bash
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Run Real-Time Service & Prefect Server
A helper script is provided to spin up both the Prefect Server and the FastAPI application concurrently, with automatic clean-up of background processes upon exit:
```bash
chmod +x run_realtime.sh
./run_realtime.sh
```

### 3. Run Bulk ETL Manually
Make sure the Prefect server is running first, then trigger the ETL pipeline:
```bash
./venv/bin/python run_etl.py
```

### 4. Check Python Syntax Compilation
Verify all python scripts compile successfully without syntax errors:
```bash
./venv/bin/python -m py_compile etl/consumers_etl.py etl/offers_etl.py
```

---

## 🎛️ Production Deployment (Ubuntu Systemd)

To run the real-time sync engine and Prefect server continuously on production, use the systemd service template `mcd_processor.service`:

1. Copy the service file to systemd:
   ```bash
   sudo cp mcd_processor.service /etc/systemd/system/mcd_processor.service
   ```
2. Reload systemd daemon:
   ```bash
   sudo systemctl daemon-reload
   ```
3. Start and enable the service on boot:
   ```bash
   sudo systemctl enable mcd_processor
   sudo systemctl start mcd_processor
   ```
4. Manage and check status:
   ```bash
   sudo systemctl status mcd_processor
   sudo systemctl restart mcd_processor
   sudo systemctl stop mcd_processor
   ```
5. Tail log outputs:
   ```bash
   sudo journalctl -u mcd_processor -f
   ```

---

## 📝 Coding Guidelines & Best Practices

To maintain compatibility and performance, please follow these rules:

### 1. Strict Type Casting
The PostgreSQL schema uses `character varying` (String) for most columns (e.g., IDs, integers, booleans), whereas MongoDB stores them as native types. **Always explicitly cast MongoDB fields to string** (e.g., `str(doc.get("isreward")).lower()`) before writing to Postgres to prevent datatype mismatch errors.

### 2. Chunking in Bulk ETL
Do not execute 30M rows in a single SQL transaction. Always split bulk loads into chunks of **1,000,000 records** and yield control using `await asyncio.sleep(0.1)` between chunks. This allows the Prefect heartbeat to report successfully and prevents timeouts.

### 3. No Raw SQL String Execution
Never execute raw string queries directly. Always wrap raw SQL strings in SQLAlchemy's `text()` function:
```python
from sqlalchemy import text
await conn.execute(text("SELECT * FROM users"))
```

### 4. Error Visibility
Never swallow exceptions silently inside listeners or background tasks. Always print the traceback using `traceback.print_exc()` so it is recorded in the system logs and visible in `journalctl`.
