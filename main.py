from fastapi import FastAPI, HTTPException, Request
import sys
import uvicorn
from pathlib import Path
import routes.routes as routers
import configs.appConfig as appConfig
from app.exceptions.customHttpException import CustomHttpException

app = FastAPI()

app.include_router(routers.router)

@app.get("/")
async def read_root():
    return {
        "Application": appConfig.app_name,
        "Version": appConfig.app_version,
        "Description": appConfig.app_description,
    }

@app.exception_handler(HTTPException)
async def httpExceptionHandler(request: Request, ex: HTTPException):
    return await CustomHttpException.handle(request, ex)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=appConfig.app_port, reload=True)
