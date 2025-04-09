from fastapi import Request
from core.models.commonModel import HttpClientModel, HttpClientResponseModel
import configs.appConfig as appConfig
from core.helpers.httpClient import HttpClient

class CommonApiHelpers:
    async def login(request: dict):
        payload = HttpClientModel(
            url = appConfig.common_api_url,
            uri = "api/auth/login",
            data = request,
            headers = {
                "x-app-token": appConfig.app_token
            }
        )

        httpClient = await HttpClient.post(payload)
        httpClientResponseModel = HttpClientResponseModel(**httpClient)

        return httpClientResponseModel