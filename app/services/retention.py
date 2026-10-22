"""Conservative archival: active inputs stay readable, archived inputs remain reusable."""

import hashlib
import json
from datetime import timedelta

from sqlalchemy import delete, exists, func, select, update

from app.core.config import get_settings
from app.core.db import session_factory
from app.models.entities import EventArchive, File, Job, JobEvent, JobInput, now, uid
from app.services.jobs import TERMINAL
from app.storage.archives import write_gzip
from app.storage.local import LocalStorage


def input_eligible(cutoff):
    active_reference = exists(
        select(JobInput.job_id)
        .join(Job, Job.id == JobInput.job_id)
        .where(JobInput.file_id == File.id, Job.status.not_in(TERMINAL))
    )
    return (
        File.archive_key.is_(None),
        File.storage_key.like("uploads/%"),
        File.last_used_at < cutoff,
        ~active_reference,
    )


def archive_input(file_id: str, cutoff) -> bool:
    storage = LocalStorage()
    with session_factory()() as session:
        record = session.scalar(select(File).where(File.id == file_id, *input_eligible(cutoff)))
        if record is None:
            return False
    source = storage.path(record.storage_key)
    key = f"archives/inputs/{file_id}/{uid()}.csv.gz"
    digest = hashlib.sha256()
    size = 0

    def chunks():
        nonlocal size
        with source.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
                yield chunk
        if size != record.size_bytes or digest.hexdigest() != record.sha256:
            raise ValueError(f"Input integrity check failed for {file_id}")

    write_gzip(key, chunks())
    with session_factory().begin() as session:
        # create_job writes last_used_at under the same file row lock. If it wins,
        # PostgreSQL rechecks that timestamp, and this archival attempt aborts.
        updated = session.execute(
            update(File)
            .where(File.id == file_id, *input_eligible(cutoff))
            .values(archive_key=key, archived_at=now())
        )
        won = updated.rowcount == 1
    if won:
        source.unlink(missing_ok=True)
    # Losing/uncommitted objects have unique keys and are collected after the grace period.
    return won


def archive_events(job_id: str, cutoff) -> bool:
    with session_factory()() as session:
        job = session.scalar(
            select(Job).where(Job.id == job_id, Job.status.in_(TERMINAL), Job.finished_at < cutoff)
        )
        if job is None or session.get(EventArchive, job_id):
            return False
        maximum = session.scalar(select(func.max(JobEvent.id)).where(JobEvent.job_id == job_id))
        if maximum is None:
            return False
    key = f"archives/events/{job_id}/{uid()}.jsonl.gz"
    count = 0

    def chunks():
        nonlocal count
        with session_factory()() as session:
            events = session.scalars(
                select(JobEvent)
                .where(
                    JobEvent.job_id == job_id,
                    JobEvent.id <= maximum,
                )
                .order_by(JobEvent.id)
                .execution_options(yield_per=500)
            )
            for item in events:
                count += 1
                record = {
                    field: getattr(item, field)
                    for field in ("id", "job_id", "attempt_id", "event_type", "detail")
                }
                record["created_at"] = item.created_at.isoformat()
                yield (json.dumps(record, ensure_ascii=False) + "\n").encode()

    _, digest = write_gzip(key, chunks())
    with session_factory().begin() as session:
        locked = session.execute(
            update(Job)
            .where(
                Job.id == job_id,
                Job.status.in_(TERMINAL),
                Job.finished_at < cutoff,
            )
            .values(finished_at=Job.finished_at)
        )
        if locked.rowcount != 1 or session.get(EventArchive, job_id):
            return False
        session.add(
            EventArchive(
                job_id=job_id,
                storage_key=key,
                sha256=digest,
                event_count=count,
                last_event_id=maximum,
            )
        )
        session.flush()
        session.execute(delete(JobEvent).where(JobEvent.job_id == job_id, JobEvent.id <= maximum))
    return True


def archive_batch() -> dict:
    settings = get_settings()
    input_cutoff = now() - timedelta(days=settings.input_archive_days)
    event_cutoff = now() - timedelta(days=settings.event_archive_days)
    with session_factory()() as session:
        inputs = session.scalars(
            select(File.id)
            .where(*input_eligible(input_cutoff))
            .order_by(File.last_used_at)
            .limit(settings.maintenance_batch_size)
        ).all()
        jobs = session.scalars(
            select(Job.id)
            .where(
                Job.status.in_(TERMINAL),
                Job.finished_at < event_cutoff,
                ~exists(select(EventArchive.job_id).where(EventArchive.job_id == Job.id)),
                exists(select(JobEvent.id).where(JobEvent.job_id == Job.id)),
            )
            .order_by(Job.finished_at)
            .limit(settings.maintenance_batch_size)
        ).all()
    return {
        "inputs_archived": sum(archive_input(f, input_cutoff) for f in inputs),
        "event_streams_archived": sum(archive_events(j, event_cutoff) for j in jobs),
    }
