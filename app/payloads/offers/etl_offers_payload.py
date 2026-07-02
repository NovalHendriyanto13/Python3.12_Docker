from pydantic import BaseModel

class EtlOffersPayload(BaseModel):
    id: str           
    campaignid: str
    title: str    
    description: str
    category: str
    codetype: str
    status: str
    isreward: str
    whenstarts: str
    whenexpires: str
    whenlastupdated: str
    redemptiontext: str