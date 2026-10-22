"""Periodic archival and orphan collection. One dedicated process in local/Compose modes."""

import logging
import signal
import threading

from sqlalchemy import update

from app.core.config import get_settings
from app.core.db import session_factory
from app.models.entities import MaintenanceState, now
from app.observability.heartbeat import ServiceHeartbeat, prune_heartbeats, upsert
from app.services.retention import archive_batch
from scripts.cleanup import cleanup

logger = logging.getLogger(__name__)


def maintain_once() -> dict:
    with session_factory().begin() as session:
        upsert(
            session,
            MaintenanceState,
            {"id": "retention", "last_started_at": now(), "result_json": {}},
            ["id"],
            ["last_started_at"],
        )
    try:
        result = archive_batch()
        result["orphans_removed"] = len(cleanup(apply=True))
        result["heartbeats_pruned"] = prune_heartbeats()
    except Exception as exc:
        with session_factory().begin() as session:
            session.execute(
                update(MaintenanceState)
                .where(MaintenanceState.id == "retention")
                .values(last_error=f"{type(exc).__name__}: {exc}"[:2000])
            )
        raise
    with session_factory().begin() as session:
        session.execute(
            update(MaintenanceState)
            .where(MaintenanceState.id == "retention")
            .values(last_succeeded_at=now(), last_error=None, result_json=result)
        )
    return result


def main():
    logging.basicConfig(level=logging.INFO)
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    with ServiceHeartbeat("maintenance"):
        while not stop.is_set():
            try:
                logger.info("Maintenance complete: %s", maintain_once())
            except Exception:
                logger.exception("Maintenance failed; retrying next interval")
            stop.wait(get_settings().maintenance_interval_seconds)


if __name__ == "__main__":
    main()
