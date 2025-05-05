from fastapi import APIRouter, Request
from core.models.commonModel import SuccessModel, ErrorModel

router = APIRouter(
    prefix="/setup"
)

plugins = [
    { "name": "JWT", "package": "jwt_plugins" },
    { "name": "Redis", "package": "redis_plugins" },
]

@router.get("/available-core-plugins")
async def availableList():
    availablePlugin = list(map(lambda plugin: plugin["name"], plugins))
    success = SuccessModel(
        success=True,
        message= "Success to fetch API",
        data={ "plugins": availablePlugin },
        code=200
    )
    return success

@router.post("/install")
async def pluginsInstall(request: Request):
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

