import socket
import time
import traceback
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from configs import app_config
from contextlib import asynccontextmanager

_ssh_tunnel = None


_TUNNEL_RETRIES = 3
_TUNNEL_RETRY_DELAY = 2


def _local_bind_in_use():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(2)
        return sock.connect_ex((app_config.db_local_bind_host, app_config.db_local_bind_port)) == 0


def _build_tunnel():
    from sshtunnel import SSHTunnelForwarder

    return SSHTunnelForwarder(
        (app_config.db_ssh_host, app_config.db_ssh_port),
        ssh_username=app_config.db_ssh_user,
        ssh_password=app_config.db_ssh_password,
        remote_bind_address=(app_config.db_remote_host, app_config.db_remote_port),
        local_bind_address=(app_config.db_local_bind_host, app_config.db_local_bind_port),
        set_keepalive=15.0,  # bulk ETL opens many concurrent channels for a long time; without
                             # keepalives the underlying SSH session has gone stale/died mid-run
    )


def start_ssh_tunnel():
    global _ssh_tunnel

    if not app_config.db_ssh_tunnel_enabled or _ssh_tunnel is not None:
        return _ssh_tunnel

    if _local_bind_in_use():
        print(
            f"[database] SSH tunnel: {app_config.db_local_bind_host}:"
            f"{app_config.db_local_bind_port} is already forwarded by another "
            f"process, reusing it."
        )
        return None

    tunnel = None
    for attempt in range(1, _TUNNEL_RETRIES + 1):
        # a fresh forwarder per attempt: a failed start() leaves its own local
        # server bound to the port, so reusing the object just self-collides
        tunnel = _build_tunnel()
        try:
            tunnel.start()
            break
        except Exception:
            traceback.print_exc()
            try:
                tunnel.stop(force=True)
            except Exception:
                pass
            if attempt == _TUNNEL_RETRIES:
                raise
            print(f"[database] SSH tunnel attempt {attempt}/{_TUNNEL_RETRIES} failed, retrying...")
            time.sleep(_TUNNEL_RETRY_DELAY)

    print(
        f"[database] SSH tunnel up: "
        f"{app_config.db_local_bind_host}:{app_config.db_local_bind_port} -> "
        f"{app_config.db_ssh_host} -> {app_config.db_remote_host}:{app_config.db_remote_port}"
    )
    _ssh_tunnel = tunnel
    return _ssh_tunnel


def stop_ssh_tunnel():
    global _ssh_tunnel
    if _ssh_tunnel is not None:
        _ssh_tunnel.stop()
        _ssh_tunnel = None


start_ssh_tunnel()

engine = create_async_engine(app_config.database_url, echo=False)

AsyncSessionLocal = sessionmaker(
    bind= engine,
    class_= AsyncSession,
    expire_on_commit= False,
)

class Base(DeclarativeBase):
    pass

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

@asynccontextmanager
async def get_session():
    async with AsyncSessionLocal() as session:
        yield session