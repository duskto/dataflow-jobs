import hashlib
import json
from datetime import timedelta
from random import uniform

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from app.core.config import get_settings
from app.core.db import session_factory
from app.core.errors import AppError, Cancelled, LeaseLost
from app.models.entities import (
    File,
    IdempotencyKey,
    Job,
    JobAttempt,
    JobEvent,
    JobInput,
    Outbox,
    now,
    uid,
)
from app.schemas.jobs import CreateJob

TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}


def event(session, job_id: str, kind: str, detail: str, attempt_id: str | None = None):
    session.add(
        JobEvent(job_id=job_id, attempt_id=attempt_id, event_type=kind, detail=detail[:2000])
    )


def get_job(session, job_id: str, owner: str) -> Job:
    job = session.scalar(select(Job).where(Job.id == job_id, Job.owner_id == owner))
    if job is None:
        raise AppError("JOB_NOT_FOUND", "任务不存在", 404)
    return job


def create_job(
    body: CreateJob, owner: str, key: str | None = None, parent_job_id: str | None = None
) -> Job:
    payload = body.model_dump(mode="json")
    payload["parent_job_id"] = parent_job_id
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()

    def existing(session):
        record = session.get(IdempotencyKey, (owner, key)) if key else None
        if record:
            if record.request_hash != digest:
                raise AppError("IDEMPOTENCY_CONFLICT", "相同幂等键对应不同请求", 409)
            return get_job(session, record.job_id, owner)
        return None

    try:
        with session_factory().begin() as session:
            if found := existing(session):
                return found
            # Coordinate with archival finalization, in stable order to avoid deadlocks.
            for file_id in sorted(set(payload["input_file_ids"])):
                locked = session.execute(
                    update(File)
                    .where(
                        File.id == file_id,
                        File.owner_id == owner,
                        File.status == "READY",
                    )
                    .values(last_used_at=now())
                )
                if locked.rowcount != 1:
                    raise AppError("FILE_NOT_FOUND", "输入文件不存在或尚未就绪", 404)
            job = Job(
                id=uid(),
                owner_id=owner,
                steps_json=payload["steps"],
                csv_options=payload["csv_options"],
                max_attempts=body.max_attempts,
                parent_job_id=parent_job_id,
            )
            session.add(job)
            session.flush()
            session.add_all(
                [
                    JobInput(job_id=job.id, file_id=f, input_order=i)
                    for i, f in enumerate(payload["input_file_ids"])
                ]
            )
            session.add(Outbox(job_id=job.id, dispatch_generation=1))
            if key:
                session.add(
                    IdempotencyKey(owner_id=owner, key=key, request_hash=digest, job_id=job.id)
                )
            event(session, job.id, "QUEUED", "任务已创建，等待派发")
            return job
    except IntegrityError:
        # A simultaneous request with the same key can win the unique constraint.
        with session_factory()() as session:
            if found := existing(session):
                return found
        raise


def claim(job_id: str, worker_id: str) -> tuple[str, dict] | None:
    attempt_id, current = uid(), now()
    with session_factory().begin() as session:
        result = session.execute(
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == "QUEUED",
                Job.cancel_requested.is_(False),
                Job.next_run_at <= current,
                Job.attempt_count < Job.max_attempts,
            )
            .values(
                status="RUNNING",
                current_attempt_id=attempt_id,
                attempt_count=Job.attempt_count + 1,
                progress=None,
                phase="读取",
                processed_rows=0,
                error_code=None,
                error_message=None,
                lease_until=current + timedelta(seconds=get_settings().lease_seconds),
            )
        )
        if result.rowcount != 1:
            return None
        job = session.get(Job, job_id)
        session.add(
            JobAttempt(
                id=attempt_id,
                job_id=job_id,
                attempt_no=job.attempt_count,
                worker_id=worker_id[:128],
                queue_seconds=max(0, (current - job.next_run_at).total_seconds()),
            )
        )
        session.flush()
        event(session, job_id, "RUNNING", f"第 {job.attempt_count} 次执行", attempt_id)
        files = session.scalars(
            select(File)
            .join(JobInput, JobInput.file_id == File.id)
            .where(JobInput.job_id == job_id)
            .order_by(JobInput.input_order)
        ).all()
        return attempt_id, {
            "steps": job.steps_json,
            "options": job.csv_options,
            "keys": [f.storage_key for f in files],
            "inputs": [
                {
                    "storage_key": f.storage_key,
                    "archive_key": f.archive_key,
                    "size_bytes": f.size_bytes,
                    "sha256": f.sha256,
                }
                for f in files
            ],
        }


def fence(job_id: str, attempt_id: str):
    return (
        Job.id == job_id,
        Job.current_attempt_id == attempt_id,
        Job.status == "RUNNING",
        Job.lease_until > now(),
    )


def heartbeat(job_id: str, attempt_id: str, phase: str, rows: int):
    with session_factory().begin() as session:
        result = session.execute(
            update(Job)
            .where(*fence(job_id, attempt_id))
            .values(
                lease_until=now() + timedelta(seconds=get_settings().lease_seconds),
                phase=phase,
                processed_rows=rows,
            )
        )
        if result.rowcount != 1:
            raise LeaseLost
        job = session.get(Job, job_id)
        if job.cancel_requested:
            raise Cancelled
        session.execute(
            update(JobAttempt).where(JobAttempt.id == attempt_id).values(heartbeat_at=now())
        )


def finish_success(job_id: str, attempt_id: str, key: str, size: int, sha256: str) -> bool:
    with session_factory().begin() as session:
        # Lock via conditional UPDATE; the cancellation endpoint uses the same row lock.
        result = session.execute(
            update(Job)
            .where(*fence(job_id, attempt_id), Job.cancel_requested.is_(False))
            .values(phase="完成")
        )
        if result.rowcount != 1:
            return False
        job = session.get(Job, job_id)
        file = File(
            id=uid(),
            owner_id=job.owner_id,
            original_name=f"{job.id}.csv",
            storage_key=key,
            size_bytes=size,
            sha256=sha256,
        )
        session.add(file)
        session.flush()
        job.status, job.progress, job.result_file_id = "SUCCEEDED", 100, file.id
        job.finished_at, job.lease_until = now(), None
        session.execute(
            update(JobAttempt).where(JobAttempt.id == attempt_id).values(finished_at=now())
        )
        event(session, job_id, "SUCCEEDED", "结果已提交", attempt_id)
        return True


def schedule_failure(session, job: Job, code: str, message: str, retryable: bool):
    attempt_id = job.current_attempt_id
    current = now()
    if job.cancel_requested:
        job.status, job.phase = "CANCELLED", "已取消"
    elif retryable and job.attempt_count < job.max_attempts:
        job.status, job.phase = "RETRY_WAIT", "等待重试"
        delay = get_settings().retry_base_seconds * 2 ** (job.attempt_count - 1)
        job.next_run_at = current + timedelta(seconds=delay + uniform(0, delay * 0.25))
    else:
        job.status, job.phase = "FAILED", "执行失败"
    job.error_code, job.error_message = code, message[:2000]
    job.lease_until = None
    job.finished_at = current if job.status in TERMINAL else None
    session.execute(
        update(JobAttempt)
        .where(JobAttempt.id == attempt_id)
        .values(finished_at=current, error_code=code, error_message=message[:2000])
    )
    event(session, job.id, job.status, message, attempt_id)


def finish_failure(job_id: str, attempt_id: str, code: str, message: str, retryable=False):
    with session_factory().begin() as session:
        result = session.execute(
            update(Job).where(*fence(job_id, attempt_id)).values(phase="结束执行")
        )
        if result.rowcount == 1:
            schedule_failure(session, session.get(Job, job_id), code, message, retryable)


def cancel_job(job_id: str, owner: str) -> Job:
    with session_factory().begin() as session:
        # UPDATE obtains a row lock on PostgreSQL and a write lock on SQLite.
        session.execute(
            update(Job)
            .where(Job.id == job_id, Job.owner_id == owner, Job.status.not_in(TERMINAL))
            .values(cancel_requested=True)
        )
        job = get_job(session, job_id, owner)
        if job.status in {"QUEUED", "RETRY_WAIT"}:
            job.status, job.phase, job.finished_at = "CANCELLED", "已取消", now()
            event(session, job_id, "CANCELLED", "任务在开始执行前被取消")
        elif job.status == "RUNNING":
            event(
                session,
                job_id,
                "CANCEL_REQUESTED",
                "取消请求已接收，等待 Worker 停止",
                job.current_attempt_id,
            )
        return job
