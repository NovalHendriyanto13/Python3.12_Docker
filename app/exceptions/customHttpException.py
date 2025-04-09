from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse

class CustomHttpException:
    async def handle(request: Request, ex: HTTPException):
        return JSONResponse(
            status_code= ex.status_code,
            content= ex.detail,
            headers= ex.headers
        )