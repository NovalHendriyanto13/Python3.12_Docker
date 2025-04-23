import os
from sqlalchemy import create_engine

# Replace with your SQL Server details
SERVER   = os.getenv("DB_HOST", "")
DATABASE = os.getenv("DB_DATABASE", "")
USERNAME = os.getenv("DB_USENAME", "")
PASSWORD = os.getenv("DB_PASSWORD", "")

# ODBC Driver (make sure it's installed on your system)
DRIVER = "ODBC Driver 17 for SQL Server"

connection_string = f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER.replace(' ', '+')}"

engine = create_engine(connection_string)
