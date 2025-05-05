from pydantic import BaseModel
from typing import Optional

class ErrorModel(BaseModel):
    success: Optional[bool] = False
    code: int
    message: str
    data: dict

class SuccessModel(BaseModel):
    success: Optional[bool] = True
    code: int
    message: str
    data: dict

class HttpClientModel(BaseModel):
    url: str
    uri: Optional[str]
    headers: Optional[dict]
    data: Optional[dict]

class HttpClientResponseModel(BaseModel):
    success: bool
    code: int
    message: str
    data: dict
    