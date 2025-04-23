from fastapi.routing import APIRoute
from fastapi import Request, Response, HTTPException
from core.models.httpModel import ErrorModel
import configs.appConfig as appConfig

class CustomHeader(APIRoute):
    def get_route_handler(self):
        original_handler = super().get_route_handler()

        async def handle(request: Request) -> Response:
            headers = request.headers

            response: Response = await original_handler(request)

            return response
        
        return handle