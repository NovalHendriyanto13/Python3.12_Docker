import os
from dotenv import load_dotenv

load_dotenv()

app_name = os.getenv("APP_NAME", "")
app_env =  os.getenv("APP_ENV", "development")
app_port = int(os.getenv("APP_PORT", 3000))
database_url = os.getenv("DATABASE_URL", "")
mongo_uri = os.getenv("MONGO_URI", "")
mongo_db = os.getenv("MONGO_DB", "")
mongo_prefix = os.getenv("MONGO_PREFIX", "")
database_url_pdc = os.getenv("DATABASE_URL_PDC", "")
etl_source = os.getenv("ETL_SOURCE", "")
databricks_server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME", "")
databricks_http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
databricks_token = os.getenv("DATABRICKS_TOKEN", "")
databricks_catalog = os.getenv("DATABRICKS_CATALOG", "")
databricks_schema = os.getenv("DATABRICKS_SCHEMA", "")
timedelta_days = int(os.getenv("TIMEDELTA_DAYS", 0))