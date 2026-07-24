from pydantic import BaseModel, Field
from typing import Optional

class SyncRequest(BaseModel):
    per_batch: int = Field(default=1000)           
    parameters: Optional[dict] = None