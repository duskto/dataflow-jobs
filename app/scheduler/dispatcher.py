import logging
import signal
import threading

from sqlalchemy import select, update

from app.core.config import get_settings
from app.core.db import session_factory
from app.models.entities import Job, Outbox, now
from app.observability.heartbeat import ServiceHeartbeat
from app.scheduler.recovery import recover

logger = logging.getLogger(__name__)


def publish(job_id: str):
    if get_settings().execution_mode == "celery":
        from app.workers.celery_app import celery_app

        celery_app.send_task("dataflow.run_job", args=[job_id], retry=False)
    # In local mode a separate worker polls the DB, never executes inside the API.


def dispatch_once(sender=publish) -> int:
    with session_factory()() as session:
        pending = session.execute(
            select(Outbox.id, Outbox.job_id)
            .join(Job)
            .where(
                Outbox.sent_at.is_(None),
                Job.status == "QUEUED",
            )
            .order_by(Outbox.created_at)
            .limit(100)
        ).all()
    sent = 0
    for outbox_id, job_id in pending:
        try:
            sender(job_id)
        except Exception:
            logger.exception("Broker unavailable; keeping outbox=%s", outbox_id)
            break
        with session_factory().begin() as session:
            session.execute(update(Outbox).where(Outbox.id == outbox_id).values(sent_at=now()))
        sent += 1
    return sent


def main():
    logging.basicConfig(level=logging.INFO)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    with ServiceHeartbeat("dispatcher"):
        while not stop.is_set():
            try:
                recover()
                dispatch_once()
            except Exception:
                logger.exception("Dispatcher cycle failed; retrying next cycle")
            stop.wait(get_settings().dispatch_interval)


if __name__ == "__main__":
    main()
