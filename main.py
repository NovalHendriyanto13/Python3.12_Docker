from fastapi import FastAPI, HTTPException, Request
import uvicorn
from contextlib import asynccontextmanager
import configs.app_config as appConfig
from configs.database import Base, engine
import exceptions.custom_http_exception as CustomHttpException
import routes.routes as routers

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield

app = FastAPI(lifespan=lifespan)

app.include_router(routers.router)

@app.get('/')
async def root():
    return { "message": "OK" }

@app.exception_handler(HTTPException)
async def httpExceptionHandler(request: Request, ex: HTTPException):
    return await CustomHttpException.handle(request, ex)

if __name__ == "__main__":
    is_dev = appConfig.app_env == "development"
    uvicorn.run("main:app", host="0.0.0.0", port=appConfig.app_port, reload=True)