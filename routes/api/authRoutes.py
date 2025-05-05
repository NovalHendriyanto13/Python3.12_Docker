from fastapi import APIRouter, Request
import configs.appConfig as appConfig
from app.controllers.api.authController import AuthController
from app.middlewares.customHeader import CustomHeader
from core.models.commonModel import SuccessModel

router = APIRouter(
    prefix="/auth", 
    route_class= CustomHeader
)

@router.get("/")
async def index():
    success = SuccessModel(
        success= True,
        message= "Success to fetch API",
        data= {},
        code= 200
    )
    return success

@router.post("/login")
async def login(request: Request):
    return {}
