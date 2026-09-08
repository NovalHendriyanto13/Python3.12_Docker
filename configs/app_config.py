import os
from dotenv import load_dotenv

load_dotenv()

app_name = os.getenv("APP_NAME", "")
app_env =  os.getenv("APP_ENV", "development")
app_port = int(os.getenv("APP_PORT", 3000))
database_url = os.getenv("DATABASE_URL", "")
mongo_uri = os.getenv("MONGO_URI", "")
mongo_db = os.getenv("MONGO_DB", "")
database_url_pdc = os.getenv("DATABASE_URL_PDC", "")
etl_source = os.getenv("ETL_SOURCE", "")

db_ssh_tunnel_enabled = os.getenv("DB_SSH_TUNNEL_ENABLED", "false").lower() == "true"
db_ssh_host = os.getenv("DB_SSH_HOST", "")
db_ssh_port = int(os.getenv("DB_SSH_PORT", 22))
db_ssh_user = os.getenv("DB_SSH_USER", "")
db_ssh_password = os.getenv("DB_SSH_PASSWORD", "")
db_remote_host = os.getenv("DB_REMOTE_HOST", "")
db_remote_port = int(os.getenv("DB_REMOTE_PORT", 5432))
db_local_bind_host = os.getenv("DB_LOCAL_BIND_HOST", "127.0.0.1")
db_local_bind_port = int(os.getenv("DB_LOCAL_BIND_PORT", 5432))