from datetime import timedelta

from sqlalchemy import select, update

from app.core.config import get_settings
from app.core.db import session_factory
from app.models.entities import Job, Outbox, now
from app.services.jobs import event, schedule_failure


def recover() -> int:
    recovered = 0
    current = now()
    with session_factory()() as session:
        expired = session.execute(
            select(Job.id, Job.current_attempt_id)
            .where(Job.status == "RUNNING", Job.lease_until <= current)
            .limit(100)
        ).all()
    for job_id, attempt_id in expired:
        with session_factory().begin() as session:
            result = session.execute(
                update(Job)
                .where(
                    Job.id == job_id,
                    Job.status == "RUNNING",
                    Job.current_attempt_id == attempt_id,
                    Job.lease_until <= current,
                )
                .values(phase="恢复中")
            )
            if result.rowcount == 1:
                schedule_failure(
                    session,
                    session.get(Job, job_id),
                    "LEASE_EXPIRED",
                    "Worker 租约到期，按重试策略恢复",
                    True,
                )
                recovered += 1
    current = now()
    with session_factory()() as session:
        ready = session.scalars(
            select(Job.id).where(Job.status == "RETRY_WAIT", Job.next_run_at <= current).limit(100)
        ).all()
    for job_id in ready:
        with session_factory().begin() as session:
            result = session.execute(
                update(Job)
                .where(
                    Job.id == job_id,
                    Job.status == "RETRY_WAIT",
                    Job.next_run_at <= current,
                    Job.cancel_requested.is_(False),
                )
                .values(
                    status="QUEUED",
                    phase="等待执行",
                    dispatch_generation=Job.dispatch_generation + 1,
                )
            )
            if result.rowcount == 1:
                job = session.get(Job, job_id)
                session.add(Outbox(job_id=job_id, dispatch_generation=job.dispatch_generation))
                event(session, job_id, "QUEUED", "重试已就绪")
    # Re-send long queued jobs even if Redis lost a previously accepted message.
    cutoff = current - timedelta(seconds=max(60, get_settings().lease_seconds * 2))
    with session_factory().begin() as session:
        queued = select(Job.id).where(Job.status == "QUEUED")
        session.execute(
            update(Outbox)
            .where(Outbox.job_id.in_(queued), Outbox.sent_at < cutoff)
            .values(sent_at=None)
        )
    return recovered
