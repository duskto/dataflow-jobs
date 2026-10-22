import secrets
from pathlib import Path
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query, UploadFile
from fastapi.responses import FileResponse, PlainTextResponse
from sqlalchemy import select, text

from app.core.config import get_settings
from app.core.db import get_engine, session_factory
from app.core.errors import AppError
from app.models.entities import EventArchive, File, Job, JobAttempt, JobEvent, JobInput
from app.observability.metrics import render, snapshot
from app.schemas.jobs import CreateJob
from app.services.jobs import cancel_job, create_job, get_job
from app.storage.local import LocalStorage

router = APIRouter(prefix="/api/v1")


def owner(authorization: Annotated[str | None, Header()] = None) -> str:
    settings = get_settings()
    if settings.api_token and not secrets.compare_digest(
        authorization or "", f"Bearer {settings.api_token}"
    ):
        raise AppError("UNAUTHORIZED", "需要有效的演示账号令牌", 401)
    return settings.owner_id


Owner = Annotated[str, Depends(owner)]


def job_view(job: Job) -> dict:
    fields = (
        "id",
        "status",
        "phase",
        "progress",
        "processed_rows",
        "attempt_count",
        "max_attempts",
        "current_attempt_id",
        "cancel_requested",
        "result_file_id",
        "parent_job_id",
        "error_code",
        "error_message",
        "created_at",
        "finished_at",
        "next_run_at",
        "lease_until",
        "csv_options",
    )
    return {**{field: getattr(job, field) for field in fields}, "steps": job.steps_json}


def file_view(file: File) -> dict:
    return {
        "file_id": file.id,
        "original_name": file.original_name,
        "size": file.size_bytes,
        "sha256": file.sha256,
        "status": file.status,
        "created_at": file.created_at,
        "archived_at": file.archived_at,
    }


@router.post("/files", status_code=201)
def upload(file: UploadFile, user: Owner):
    name = Path((file.filename or "input.csv").replace("\\", "/")).name[:255]
    if not name.lower().endswith(".csv"):
        raise AppError("INVALID_FILE_TYPE", "仅支持 CSV 文件", 422)
    storage = LocalStorage()
    try:
        key, size, digest = storage.upload(file.file)
    finally:
        file.file.close()
    with session_factory().begin() as session:
        record = File(
            owner_id=user, original_name=name, storage_key=key, size_bytes=size, sha256=digest
        )
        session.add(record)
        session.flush()
        response = file_view(record)
    return response


@router.get("/files/{file_id}")
def file_info(file_id: UUID, user: Owner):
    with session_factory()() as session:
        file = session.scalar(select(File).where(File.id == str(file_id), File.owner_id == user))
        if file is None:
            raise AppError("FILE_NOT_FOUND", "文件不存在", 404)
        return file_view(file)


@router.post("/jobs", status_code=202)
def submit(
    body: CreateJob,
    user: Owner,
    idempotency_key: Annotated[str | None, Header(max_length=128, min_length=1)] = None,
):
    return job_view(create_job(body, user, idempotency_key))


@router.get("/jobs")
def jobs(
    user: Owner,
    status: str | None = None,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0, le=100000),
):
    if status and status not in {
        "QUEUED",
        "RUNNING",
        "RETRY_WAIT",
        "SUCCEEDED",
        "FAILED",
        "CANCELLED",
    }:
        raise AppError("INVALID_STATUS", "未知任务状态", 422)
    with session_factory()() as session:
        query = select(Job).where(Job.owner_id == user)
        if status:
            query = query.where(Job.status == status)
        items = session.scalars(
            query.order_by(Job.created_at.desc(), Job.id).offset(offset).limit(limit + 1)
        ).all()
        return {
            "items": [job_view(job) for job in items[:limit]],
            "next_offset": offset + limit if len(items) > limit else None,
        }


@router.get("/jobs/{job_id}")
def job_info(job_id: UUID, user: Owner):
    with session_factory()() as session:
        job = get_job(session, str(job_id), user)
        attempts = session.scalars(
            select(JobAttempt).where(JobAttempt.job_id == job.id).order_by(JobAttempt.attempt_no)
        ).all()
        return {
            **job_view(job),
            "attempts": [
                {
                    field: getattr(a, field)
                    for field in (
                        "id",
                        "attempt_no",
                        "worker_id",
                        "queue_seconds",
                        "started_at",
                        "finished_at",
                        "error_code",
                        "error_message",
                    )
                }
                for a in attempts
            ],
        }


@router.post("/jobs/{job_id}/cancel")
def cancel(job_id: UUID, user: Owner):
    return job_view(cancel_job(str(job_id), user))


@router.post("/jobs/{job_id}/retry", status_code=202)
def retry(
    job_id: UUID,
    user: Owner,
    idempotency_key: Annotated[str | None, Header(max_length=128, min_length=1)] = None,
):
    with session_factory()() as session:
        job = get_job(session, str(job_id), user)
        if job.status != "FAILED":
            raise AppError("NOT_RETRYABLE", "仅失败任务可以手动重试", 409)
        inputs = session.scalars(
            select(JobInput.file_id).where(JobInput.job_id == job.id).order_by(JobInput.input_order)
        ).all()
        body = CreateJob(
            input_file_ids=inputs,
            steps=job.steps_json,
            csv_options=job.csv_options,
            max_attempts=job.max_attempts,
        )
    return job_view(create_job(body, user, idempotency_key, parent_job_id=str(job_id)))


@router.get("/jobs/{job_id}/events")
def events(
    job_id: UUID, user: Owner, after: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=100)
):
    with session_factory()() as session:
        get_job(session, str(job_id), user)
        records = session.scalars(
            select(JobEvent)
            .where(
                JobEvent.job_id == str(job_id),
                JobEvent.id > after,
            )
            .order_by(JobEvent.id)
            .limit(limit + 1)
        ).all()
        items = [
            {k: getattr(e, k) for k in ("id", "attempt_id", "event_type", "detail", "created_at")}
            for e in records[:limit]
        ]
        archive = session.get(EventArchive, str(job_id))
        return {
            "archive": (
                {
                    "event_count": archive.event_count,
                    "sha256": archive.sha256,
                    "created_at": archive.created_at,
                }
                if archive
                else None
            ),
            "items": items,
            "next_cursor": items[-1]["id"] if items else after,
            "has_more": len(records) > limit,
        }


@router.get("/jobs/{job_id}/result")
def result(job_id: UUID, user: Owner):
    with session_factory()() as session:
        job = get_job(session, str(job_id), user)
        if job.status != "SUCCEEDED":
            raise AppError("RESULT_NOT_READY", "任务尚未成功，无可下载结果", 409)
        file = session.scalar(
            select(File).where(File.id == job.result_file_id, File.owner_id == user)
        )
        path = LocalStorage().path(file.storage_key)
        if not path.is_file():
            raise AppError("RESULT_MISSING", "结果文件不可用", 503)
        return FileResponse(path, filename=file.original_name, media_type="text/csv")


@router.get("/health/live")
def live():
    return {"status": "ok"}


@router.get("/health/ready")
def ready():
    try:
        with get_engine().connect() as connection:
            connection.execute(text("SELECT 1 FROM jobs LIMIT 1"))
        storage = LocalStorage()
        probe = storage.path(f".probe-{secrets.token_hex(8)}")
        try:
            probe.write_bytes(b"ready")
        finally:
            probe.unlink(missing_ok=True)
        if get_settings().execution_mode == "celery":
            from redis import Redis

            Redis.from_url(
                get_settings().redis_url, socket_connect_timeout=2, socket_timeout=2
            ).ping()
    except Exception:
        raise AppError("NOT_READY", "数据库、迁移、存储或队列尚未就绪", 503) from None
    return {"status": "ready", "execution_mode": get_settings().execution_mode}


@router.get("/jobs/{job_id}/events/archive")
def event_archive(job_id: UUID, user: Owner):
    with session_factory()() as session:
        get_job(session, str(job_id), user)
        archive = session.get(EventArchive, str(job_id))
        if archive is None:
            raise AppError("ARCHIVE_NOT_FOUND", "尚无归档日志", 404)
        path = LocalStorage().path(archive.storage_key)
        if not path.is_file():
            raise AppError("ARCHIVE_UNAVAILABLE", "归档日志暂不可用", 503)
        return FileResponse(
            path, filename=f"{job_id}-events.jsonl.gz", media_type="application/gzip"
        )


@router.get("/ops")
def operations(user: Owner):
    return snapshot(user)


@router.get("/metrics", response_class=PlainTextResponse)
def metrics(user: Owner):
    return PlainTextResponse(render(snapshot(user)), media_type="text/plain; version=0.0.4")
