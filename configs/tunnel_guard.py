"""Auto-heals the local SSH tunnels to remote MongoDB/PostgreSQL.

Both databases are only reachable through SSH port-forwards (see
/var/www/html/works/makes/makefile). Those tunnels are plain background
ssh processes that can die (port stops listening) or go stale (still
listening locally but the upstream forward is dead, so the handshake
just hangs) on network blips/reboots, with no signal to the app.

This does a real protocol-level check (Mongo ping / Postgres SELECT 1)
rather than just a TCP connect, because a stale tunnel still accepts the
TCP connection locally even though it never completes anything. If the
check fails, it kills whatever is holding the local port and restarts
the tunnel via the matching `make` target.
"""

import asyncio
import subprocess
from urllib.parse import urlparse

MAKES_DIR = "/var/www/html/works/makes"
LOCAL_HOSTS = ("localhost", "127.0.0.1")
CHECK_TIMEOUT = 5
RESTART_RETRIES = 10
RESTART_DELAY = 1

# local port -> (make target that (re)establishes the tunnel, db kind)
TUNNELS = {
    27018: ("access_mcd_cdp_mongo_to_local", "mongo"),
    5435: ("access_mcd_cdp_postgre_to_local", "postgres"),
}


async def _check_mongo(uri):
    from motor.motor_asyncio import AsyncIOMotorClient

    client = AsyncIOMotorClient(
        uri,
        serverSelectionTimeoutMS=CHECK_TIMEOUT * 1000,
        connectTimeoutMS=CHECK_TIMEOUT * 1000,
    )
    try:
        await client.admin.command("ping")
        return True
    except Exception:
        return False
    finally:
        client.close()


async def _check_postgres(uri):
    import asyncpg

    dsn = uri.replace("postgresql+asyncpg://", "postgresql://", 1)
    try:
        conn = await asyncio.wait_for(asyncpg.connect(dsn=dsn), timeout=CHECK_TIMEOUT)
    except Exception:
        return False
    try:
        await conn.fetchval("SELECT 1")
        return True
    except Exception:
        return False
    finally:
        await conn.close()


async def _is_healthy(uri, kind):
    return await (_check_mongo(uri) if kind == "mongo" else _check_postgres(uri))


def _kill_port_holder(port):
    subprocess.run(
        ["fuser", "-k", "-n", "tcp", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _start_tunnel(target):
    subprocess.Popen(
        ["make", target],
        cwd=MAKES_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


async def _ensure_tunnel_for_uri(uri, label):
    parsed = urlparse(uri)
    host, port = parsed.hostname, parsed.port

    if host not in LOCAL_HOSTS or port not in TUNNELS:
        return

    target, kind = TUNNELS[port]

    if await _is_healthy(uri, kind):
        return

    print(f"[tunnel_guard] {label} tunnel on {host}:{port} is unhealthy, restarting via make {target}...")
    _kill_port_holder(port)
    _start_tunnel(target)

    for _ in range(RESTART_RETRIES):
        await asyncio.sleep(RESTART_DELAY)
        if await _is_healthy(uri, kind):
            print(f"[tunnel_guard] {label} tunnel restored on {host}:{port}.")
            return

    raise RuntimeError(
        f"[tunnel_guard] Could not bring up the {label} tunnel "
        f"('make {target}' in {MAKES_DIR}) after {RESTART_RETRIES}s. "
        f"Check the SSH key / bastion access manually."
    )


async def ensure_all_tunnels():
    from configs.app_config import mongo_uri, database_url

    await _ensure_tunnel_for_uri(mongo_uri, "MongoDB")
    await _ensure_tunnel_for_uri(database_url, "PostgreSQL")
