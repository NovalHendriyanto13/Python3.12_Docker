from fastapi.routing import APIRoute
from fastapi import Request, Response, HTTPException
from core.models.commonModel import ErrorModel
import configs.appConfig as appConfig

class CustomHeader(APIRoute):
    def get_route_handler(self):
        original_handler = super().get_route_handler()

        async def handle(request: Request) -> Response:
            headers = request.headers

            if headers.get('x-app-token') is None:
                errorModel = ErrorModel(
                    success= False,
                    message= "App Token is Missing",
                    data= {},
                    code=400
                )
                raise HTTPException(
                    status_code=400,
                    detail=errorModel.dict(),
                    headers={"X-Custom-Error": "TokenMissing"}
                )
            elif headers.get('x-app-token') != appConfig.app_token :
                errorModel = ErrorModel(
                    success= False,
                    message= "Invalid App Token",
                    data= {},
                    code=400
                )
                raise HTTPException(
                    status_code=400,
                    detail=errorModel.dict(),
                    headers={"X-Custom-Error": "TokenInvalid"}
                )

            response: Response = await original_handler(request)

            return response
        
        return handle