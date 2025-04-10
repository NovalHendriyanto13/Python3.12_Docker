import os

app_name = os.getenv("APP_NAME", "")
app_version = os.getenv("VERSION", "1.0")
app_description = os.getenv("APP_DESCRIPTION", "")
app_port = os.getenv("APP_PORT", "8080")
app_token = os.getenv("APP_TOKEN", "")
common_api_url = os.getenv("COMMON_API_URL", "")
jwt_private_key = os.getenv("JWT_PRIVATE_KEY", "")
jwt_public_key = os.getenv("JWT_PUBLIC_KEY", "")
jwt_alogarithm = os.getenv("JWT_ALOGARITHM", "")
huggingface_access_token = os.getenv("HUGGINGFACE_ACCESS_TOKEN", "")