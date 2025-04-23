import os

app_name = os.getenv("APP_NAME", "")
app_version = os.getenv("VERSION", "1.0")
app_description = os.getenv("APP_DESCRIPTION", "")
app_port = os.getenv("APP_PORT", "8080")
app_token = os.getenv("APP_TOKEN", "")