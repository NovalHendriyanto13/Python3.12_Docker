from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from app.models.consumers_model import Consumers

async def upsert_consumers(db: AsyncSession, mongo_doc: dict):
    stmt = insert(Consumers).values(

        reportingid = mongo_doc["reportingid"],  
        firstname = mongo_doc["firstname"],
        lastname = mongo_doc["lastname"],
        fullname = mongo_doc["fullname"],
        emailaddress = mongo_doc["emailaddress"],
        registrationtype = mongo_doc["registrationtype"], 
        postcode = mongo_doc["postcode"],
        creationdate = mongo_doc["creationdate"],
        modifieddate = mongo_doc["modifieddate"],
        dateofbirth = mongo_doc["dateofbirth"],
        gender = mongo_doc["gender"],
        isdeactivated  = mongo_doc["isdeactivated"],
        deactivationdate = mongo_doc["deactivationdate"],
        lastknowndeviceid = mongo_doc["lastknowndeviceid"],
        phonenumber = mongo_doc["phonenumber"],
        consumertype = mongo_doc["consumertype"],

    ).on_conflict_do_update(
        index_elements=["reportingid"],
        set_={
            "firstname": mongo_doc["firstname"],
            "lastname": mongo_doc["lastname"],
            "fullname": mongo_doc["fullname"],
            "emailaddress": mongo_doc["emailaddress"],
            "registrationtype": mongo_doc["registrationtype"], 
            "postcode": mongo_doc["postcode"],
            "creationdate": mongo_doc["creationdate"],
            "modifieddate": mongo_doc["modifieddate"],
            "dateofbirth": mongo_doc["dateofbirth"],
            "gender": mongo_doc["gender"],
            "isdeactivated": mongo_doc["isdeactivated"],
            "deactivationdate": mongo_doc["deactivationdate"],
            "lastknowndeviceid": mongo_doc["lastknowndeviceid"],
            "phonenumber": mongo_doc["phonenumber"],
            "consumertype": mongo_doc["consumertype"],
        }
    )
    await db.execute(stmt)
    await db.commit()
