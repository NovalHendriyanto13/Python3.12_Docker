import httpx
from core.models.commonModel import HttpClientModel

class HttpClient:
    async def post(model: HttpClientModel):
        async with httpx.AsyncClient(timeout=10.0) as client:
            url = model.url + "/" + model.uri
            response = await client.post(
                url,
                json = model.data,
                headers = model.headers
            )
            
            return response.json()