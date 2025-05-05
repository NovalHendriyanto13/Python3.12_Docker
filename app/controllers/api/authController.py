from fastapi import Request, HTTPException
from models.userModel import UserLoginModel
from core.models.commonModel import SuccessModel, ErrorModel

class AuthController :
    async def login(request: Request):
        requestData = await request.json()
        userLoginModel = UserLoginModel(**requestData)


        # common = await CommonApiHelpers.login(userLoginModel.dict())

        # if common.success == False:
        #     errorModel = ErrorModel(
        #         success= False,
        #         message= common.message,
        #         data= {},
        #         code=400
        #     )
        #     raise HTTPException(
        #         status_code=400,
        #         detail=errorModel.dict(),
        #     ) 

        # return common
        return {}