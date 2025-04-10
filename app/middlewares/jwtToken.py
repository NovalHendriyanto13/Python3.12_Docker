from fastapi import Request, HTTPException
from core.models.commonModel import ErrorModel
from jose import jwt, JWTError
import configs.appConfig as appConfig

class JWTToken:
    async def verify(request: Request):
        with open(appConfig.jwt_private_key, "r") as f:
            PRIVATE_KEY = f.read()

        with open(appConfig.jwt_public_key, "r") as f:
            PUBLIC_KEY = f.read()

        ALGORITHM = appConfig.jwt_alogarithm

        headers = request.headers
        auth = headers.get('Authorization')

        try:
            token = ''
            if not auth or not auth.startswith("Bearer "):
                errorModel = ErrorModel(
                    success= False,
                    message= "Missing Token",
                    data= {},
                    code=400
                )
                raise HTTPException(status_code=401, detail=errorModel.dict())
                
            token = auth[7:]
            decoded = jwt.decode(token, PUBLIC_KEY, algorithms=[ALGORITHM])
            
            return decoded
        except JWTError:
            errorModel = ErrorModel(
                success= False,
                message= "Invalid Token",
                data= {},
                code=400
            )
            raise HTTPException(
                status_code=400,
                detail=errorModel.dict(),
                headers={"X-Custom-Error": "TokenInvalid"}
            )
