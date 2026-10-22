"""Database-backed liveness for idle and busy worker processes and schedulers."""

import logging
import os
import socket
import threading
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import delete, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.core.config import get_settings
from app.core.db import get_engine, session_factory
from app.models.entities import ComponentHeartbeat, now

logger = logging.getLogger(__name__)


def upsert(session, model, values: dict, keys: list[str], changed: list[str]):
    insert = sqlite_insert if get_engine().dialect.name == "sqlite" else pg_insert
    statement = insert(model).values(**values)
    session.execute(
        statement.on_conflict_do_update(
            index_elements=keys,
            set_={name: getattr(statement.excluded, name) for name in changed},
        )
    )


class ServiceHeartbeat:
    def __init__(self, role: str):
        self.role = role
        self.id = f"{role}:{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:8]}"
        self.started_at = now()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._loop, name=f"{role}-heartbeat", daemon=True)

    def pulse(self):
        with session_factory().begin() as session:
            upsert(
                session,
                ComponentHeartbeat,
                {
                    "id": self.id,
                    "role": self.role,
                    "mode": get_settings().execution_mode,
                    "last_seen": now(),
                    "started_at": self.started_at,
                    "stopped_at": None,
                },
                ["id"],
                ["last_seen", "stopped_at"],
            )

    def _loop(self):
        while not self.stop_event.is_set():
            try:
                self.pulse()
            except Exception:
                logger.exception("Component heartbeat unavailable role=%s", self.role)
            self.stop_event.wait(get_settings().heartbeat_interval_seconds)

    def start(self):
        self.thread.start()
        return self

    def close(self):
        self.stop_event.set()
        self.thread.join(timeout=get_settings().db_connect_timeout_seconds + 1)
        if self.thread.is_alive():
            # No further pulse is scheduled; the last heartbeat naturally expires.
            return
        try:
            with session_factory().begin() as session:
                session.execute(
                    update(ComponentHeartbeat)
                    .where(ComponentHeartbeat.id == self.id)
                    .values(stopped_at=now())
                )
        except Exception:
            logger.warning("Could not mark heartbeat stopped role=%s", self.role)

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.close()


def prune_heartbeats():
    with session_factory().begin() as session:
        return session.execute(
            delete(ComponentHeartbeat).where(
                ComponentHeartbeat.last_seen < now() - timedelta(days=7)
            )
        ).rowcount
