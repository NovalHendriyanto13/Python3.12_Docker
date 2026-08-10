from sqlalchemy.ext.asyncio import AsyncSession
from configs.mongo import mongo_conn
from app.models.dim_product_pillars import DimProductPillars
from app.models.dim_products import DimProducts
from app.helpers.app_helper import to_deterministic_uuid
from app.helpers.db_helper import _upsert_batch

def _build_pillar(pillar_name, pillar_description):
    if not pillar_name or not pillar_description:
        return None

    return {
        "product_pillar_key": to_deterministic_uuid("dim_product_pillars", pillar_name, pillar_description),
        "pillar_name": pillar_name,
        "pillar_description": pillar_description,
    }

def _build_product(mongo_doc, product_pillar_key):
    product_id = mongo_doc.get('key_id')
    if not product_id:
        return None

    return {
        "product_key": to_deterministic_uuid("dim_products", product_id),
        "product_pillar_key": product_pillar_key,
        "product_id": str(product_id),
        "product_name": mongo_doc.get('product_name'),
        "main_category": mongo_doc.get('main_category'),
        "main_sub_menu": mongo_doc.get('menu_sub_menu'),
        "sub_category": mongo_doc.get('sub_category'),
    }

async def upsert_products(db: AsyncSession, mongo_doc: dict):
    try:
        pillar = _build_pillar(mongo_doc.get('product_pillars'), mongo_doc.get('pillar_details'))
        pillar_key = pillar["product_pillar_key"] if pillar else None

        if pillar:
            await _upsert_batch(
                db=db,
                model=DimProductPillars,
                data_list=[pillar],
                index_elements=["product_pillar_key"],
                exclude_from_update=["product_pillar_key"]
            )

        product = _build_product(mongo_doc, pillar_key)
        if product:
            await _upsert_batch(
                db=db,
                model=DimProducts,
                data_list=[product],
                index_elements=["product_key"],
                exclude_from_update=["product_key"]
            )

        await db.commit()
    except Exception:
        await db.rollback()
        raise

async def product_init(
    db: AsyncSession
):
    batch_size = 1000
    last_id = None

    while True:
        current_query = {"_id": {"$gt": last_id}} if last_id is not None else {}

        cursor = mongo_conn["mcd_products"].find(current_query).sort("_id", 1).limit(batch_size)
        result = await cursor.to_list(length=batch_size)

        if not result:
            break

        # Dedupe within the batch: Postgres' ON CONFLICT DO UPDATE cannot
        # touch the same conflict target twice in one statement, and many
        # products share the exact same pillar combination.
        pillars_by_key = {}
        products_by_key = {}

        for mongo_doc in result:
            pillar = _build_pillar(mongo_doc.get('product_pillars'), mongo_doc.get('pillar_details'))
            pillar_key = pillar["product_pillar_key"] if pillar else None
            if pillar:
                pillars_by_key[pillar_key] = pillar

            product = _build_product(mongo_doc, pillar_key)
            if product:
                products_by_key[product["product_key"]] = product

        try:
            if pillars_by_key:
                await _upsert_batch(
                    db=db,
                    model=DimProductPillars,
                    data_list=list(pillars_by_key.values()),
                    index_elements=["product_pillar_key"],
                    exclude_from_update=["product_pillar_key"]
                )

            if products_by_key:
                await _upsert_batch(
                    db=db,
                    model=DimProducts,
                    data_list=list(products_by_key.values()),
                    index_elements=["product_key"],
                    exclude_from_update=["product_key"]
                )

            await db.commit()
            print(f"🚀 Bulk Sync Products: upserted {len(products_by_key)} products / {len(pillars_by_key)} pillars.")
        except Exception:
            await db.rollback()
            raise

        last_id = result[-1]["_id"]

        if len(result) < batch_size:
            break
