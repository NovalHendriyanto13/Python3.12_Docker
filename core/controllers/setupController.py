from fastapi import Request, HTTPException
from core.models.commonModel import SuccessModel, ErrorModel

class SetupController:
    def __init__(self):
        self.__availableList = [
            { "name": "JWT", "package": "jwt_plugins" },
            { "name": "Redis", "package": "redis_plugins" },
        ]

    async def availableList(self):
        availablePlugin = list(map(lambda plugin: plugin["name"], self.__availableList))
        success = SuccessModel(
            success=True,
            message= "Success to fetch API",
            data={ "plugins": availablePlugin },
            code=200
        )
        return success

    async def pluginInstall(self, request: Request):
        requestData = await request.json()
        pluginName = requestData.get("name")

        messageResponse = "Invalid Request, Please select the available plugin"
        match (pluginName):
            case ("JWT"):
                messageResponse = "JWT is selected"
            case ("Redis"):
                messageResponse = "Redis is selected"
            case _:
                messageResponse = messageResponse
        
        return ErrorModel(
            success= False,
            message= messageResponse,
            data= {},
            code=400
        )

# DI
def get_DI_controller():
    return SetupController()
