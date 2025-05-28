from fastapi import Request, HTTPException
from core.models.commonModel import SuccessModel, ErrorModel
from core.helpers.customHelper import findIndex, copyFile
import sys, subprocess

class SetupController:
    def __init__(self):
        self.__availableList = [
            { "name": "JWT", "package": "jwt", "plugin_name": "jwt_plugins" },
            { "name": "Redis", "package": "redis[async]", "plugin_name": "redis_plugins" },
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
        valid = False

        messageResponse = "Invalid Request, Please select the available plugin"

        index = findIndex(self.__availableList, "name", pluginName)
        if (index > -1):
            messageResponse = f"{pluginName} is selected"
            valid = True
            try:
                plugin = self.__availableList[index]["package"]
                self.installPackage(plugin)

                # pluginName = self.__availableList[index]["plugin_name"]
                # pluginPath = f"./plugins/{pluginName}"
                # self.installPackage(pluginPath)

                # destPluginDest = f""
                # copyFile(pluginPath, )

            except subprocess.CalledProcessError as e:
                messageResponse = str(e)

        if (valid):
            return SuccessModel(
                success= True,
                message= messageResponse,
                data= {},
                code=200
            )
        else:
            return ErrorModel(
                success= False,
                message= messageResponse,
                code=400
            )

    @staticmethod
    def installPackage(name):
        subprocess.check_call([sys.executable, "-m", "pip", "install", name])

    @staticmethod
    async def getInstalledPackage():
        return subprocess.check_output([sys.executable, "-m", "pip", "list"])


# DI
def get_DI_setup_controller():
    return SetupController()
