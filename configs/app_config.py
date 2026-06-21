import os
from dotenv import load_dotenv

load_dotenv()

app_name = os.getenv("APP_NAME", "")
app_env =  os.getenv("APP_ENV", "development")
app_port = int(os.getenv("APP_PORT", 3000))
database_url = os.getenv("DATABASE_URL", "")
mongo_uri = os.getenv("MONGO_URI", "")
mongo_db = os.getenv("MONGO_DB", "")