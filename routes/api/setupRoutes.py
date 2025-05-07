from fastapi import APIRouter, Request, Depends
from core.models.commonModel import SuccessModel, ErrorModel
from core.controllers.setupController import SetupController, get_DI_controller

router = APIRouter(
    prefix="/setup"
)

@router.get("/available-core-plugins")
async def availableList(controller: SetupController = Depends(get_DI_controller)):
    return await controller.availableList()

@router.post("/install")
async def pluginsInstall(request: Request, controller: SetupController = Depends(get_DI_controller)):
    return await controller.pluginInstall(request)

