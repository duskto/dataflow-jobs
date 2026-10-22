from functools import lru_cache
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.engine import make_url
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.core.config import get_settings


class Base(DeclarativeBase):
    pass


@lru_cache
def get_engine():
    url = get_settings().database_url
    if url.startswith("sqlite") and ":memory:" not in url:
        Path(url.split("///", 1)[1]).parent.mkdir(parents=True, exist_ok=True)
    connection_args = {"check_same_thread": False, "timeout": 30}
    if url.startswith("postgresql"):
        parsed = make_url(url)
        options = parsed.query.get("options", "")
        url = parsed.update_query_dict(
            {
                "options": (
                    f"{options} -c statement_timeout={get_settings().db_statement_timeout_ms}"
                ).strip()
            }
        )
        connection_args = {
            "connect_timeout": get_settings().db_connect_timeout_seconds,
            "keepalives_idle": 5,
            "keepalives_interval": 2,
            "keepalives_count": 2,
            "tcp_user_timeout": 5000,
        }
    engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args=connection_args,
    )
    if engine.dialect.name == "sqlite":

        @event.listens_for(engine, "connect")
        def sqlite_pragmas(connection, _):
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA journal_mode=WAL")

    return engine


def session_factory():
    return sessionmaker(get_engine(), expire_on_commit=False)
