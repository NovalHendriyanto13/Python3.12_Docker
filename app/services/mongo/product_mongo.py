from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete
from configs.mongo import mongo_conn
from app.models.dim_product_pillars import DimProductPillars
from app.models.dim_products import DimProducts
from app.models.dim_pillars import DimPillars
from app.helpers.app_helper import to_deterministic_uuid
from app.helpers.db_helper import _upsert_batch, _get_max_numeric_id

# product_id (key_id) is auto-filled with a sequential 6-digit string when the
# source doc doesn't have one. Kept as a per-run counter (seeded once from the
# DB) instead of re-querying MAX() per doc, since a bulk batch builds all its
# rows before any of them are inserted.
KEY_ID_WIDTH = 6

# pillar_details looks like "Beverages:Coffee|Breakfast:Muffin|Breakfast:Other" —
# segments separated by "|", each segment a "pillar_name:sub_pillar_name" pair.
# One product can belong to several pillars at once.
def _parse_pillar_segments(pillar_details):
    if not pillar_details:
        return []

    segments = []
    for raw in pillar_details.split("|"):
        raw = raw.strip()
        if not raw or ":" not in raw:
            continue

        pillar_name, sub_pillar_name = raw.split(":", 1)
        pillar_name = pillar_name.strip()
        sub_pillar_name = sub_pillar_name.strip()
        if not pillar_name or not sub_pillar_name:
            continue

        segments.append((pillar_name, sub_pillar_name))

    return segments

def _build_pillar(pillar_name, sub_pillar_name):
    return {
        "pillar_key": to_deterministic_uuid("dim_pillars", pillar_name, sub_pillar_name),
        "pillar_name": pillar_name,
        "sub_pillar_name": sub_pillar_name,
        "pillar_description": f"{pillar_name}|{sub_pillar_name}",
    }

def _build_product(mongo_doc, id_counter: dict):
    product_name = mongo_doc.get('product_name')
    if not product_name:
        return None

    product_id = mongo_doc.get('key_id')
    if not product_id:
        id_counter["value"] += 1
        product_id = str(id_counter["value"]).zfill(KEY_ID_WIDTH)

    return {
        # Keyed off product_name (the actual unique key) so it stays stable
        # across runs even when key_id is auto-generated fresh each time.
        "product_key": to_deterministic_uuid("dim_products", product_name),
        "product_id": str(product_id),
        "product_name": product_name,
        "main_category": mongo_doc.get('main_category'),
        "main_sub_menu": mongo_doc.get('menu_sub_menu'),
        "sub_category": mongo_doc.get('sub_category'),
    }

def _build_product_pillars(product_key, pillars):
    return [
        {
            "product_pillar_key": to_deterministic_uuid("dim_product_pillars", product_key, p["pillar_key"]),
            "product_key": product_key,
            "pillar_key": p["pillar_key"],
        }
        for p in pillars
    ]

async def _sync_product_pillars(db: AsyncSession, product_key, pillars):
    """Upsert the product's current pillar relations and drop any relation
    that is no longer present in the source (pillars can change over time)."""
    junctions = _build_product_pillars(product_key, pillars)
    current_pillar_keys = [p["pillar_key"] for p in pillars]

    stmt = delete(DimProductPillars).where(DimProductPillars.product_key == product_key)
    if current_pillar_keys:
        stmt = stmt.where(DimProductPillars.pillar_key.notin_(current_pillar_keys))
    await db.execute(stmt)

    if junctions:
        await _upsert_batch(
            db=db,
            model=DimProductPillars,
            data_list=junctions,
            index_elements=["product_key", "pillar_key"],
        )

async def upsert_products(db: AsyncSession, mongo_doc: dict):
    try:
        segments = _parse_pillar_segments(mongo_doc.get('pillar_details'))
        pillars = [_build_pillar(name, sub_name) for name, sub_name in segments]

        if pillars:
            await _upsert_batch(
                db=db,
                model=DimPillars,
                data_list=pillars,
                index_elements=["pillar_name", "sub_pillar_name"],
            )

        id_counter = {"value": await _get_max_numeric_id(db, DimProducts, "product_id", KEY_ID_WIDTH)}
        product = _build_product(mongo_doc, id_counter)
        if product:
            await _upsert_batch(
                db=db,
                model=DimProducts,
                data_list=[product],
                index_elements=["product_name"],
                exclude_from_update=["product_key", "product_id"]
            )

            await _sync_product_pillars(db, product["product_key"], pillars)

        await db.commit()
    except Exception:
        await db.rollback()
        raise

async def product_init(
    db: AsyncSession
):
    batch_size = 1000
    last_id = None
    id_counter = {"value": await _get_max_numeric_id(db, DimProducts, "product_id", KEY_ID_WIDTH)}

    while True:
        current_query = {"_id": {"$gt": last_id}} if last_id is not None else {}

        cursor = mongo_conn["mcd_products"].find(current_query).sort("_id", 1).limit(batch_size)
        result = await cursor.to_list(length=batch_size)

        if not result:
            break

        # Dedupe within the batch: Postgres' ON CONFLICT DO UPDATE cannot
        # touch the same conflict target twice in one statement. product_key is
        # derived from product_name (the unique key), so this also collapses
        # docs that share a product_name.
        pillars_by_key = {}
        products_by_key = {}
        pillars_by_product = {}

        for mongo_doc in result:
            product = _build_product(mongo_doc, id_counter)
            if not product:
                continue

            products_by_key[product["product_key"]] = product

            segments = _parse_pillar_segments(mongo_doc.get('pillar_details'))
            product_pillars = [_build_pillar(name, sub_name) for name, sub_name in segments]
            pillars_by_product[product["product_key"]] = product_pillars
            for p in product_pillars:
                pillars_by_key[p["pillar_key"]] = p

        try:
            if pillars_by_key:
                await _upsert_batch(
                    db=db,
                    model=DimPillars,
                    data_list=list(pillars_by_key.values()),
                    index_elements=["pillar_name", "sub_pillar_name"],
                )

            if products_by_key:
                await _upsert_batch(
                    db=db,
                    model=DimProducts,
                    data_list=list(products_by_key.values()),
                    index_elements=["product_name"],
                    exclude_from_update=["product_key", "product_id"]
                )

            product_keys_in_batch = list(products_by_key.keys())
            await db.execute(delete(DimProductPillars).where(DimProductPillars.product_key.in_(product_keys_in_batch)))

            junctions = [
                junction
                for product_key in product_keys_in_batch
                for junction in _build_product_pillars(product_key, pillars_by_product[product_key])
            ]
            if junctions:
                await _upsert_batch(
                    db=db,
                    model=DimProductPillars,
                    data_list=junctions,
                    index_elements=["product_key", "pillar_key"],
                )

            await db.commit()
            print(f"🚀 Bulk Sync Products: upserted {len(products_by_key)} products / {len(pillars_by_key)} pillars / {len(junctions)} product-pillar relations.")
        except Exception:
            await db.rollback()
            raise

        last_id = result[-1]["_id"]

        if len(result) < batch_size:
            break
