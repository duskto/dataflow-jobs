"""SQLite-friendly demo worker. Run multiple processes to exercise atomic claims."""

import logging
import signal
import threading

from sqlalchemy import select

from app.core.db import session_factory
from app.models.entities import Job, now
from app.observability.heartbeat import ServiceHeartbeat
from app.workers.runner import run_job


def main():
    logging.basicConfig(level=logging.INFO)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    with ServiceHeartbeat("worker"):
        while not stop.is_set():
            try:
                with session_factory()() as session:
                    job_id = session.scalar(
                        select(Job.id)
                        .where(
                            Job.status == "QUEUED",
                            Job.next_run_at <= now(),
                        )
                        .order_by(Job.created_at)
                        .limit(1)
                    )
                if job_id:
                    run_job(job_id)
                else:
                    stop.wait(0.5)
            except Exception:
                logging.exception("Local worker cycle failed")
                stop.wait(1)


if __name__ == "__main__":
    main()
