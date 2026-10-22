from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import TypeDecorator

from app.core.db import Base


def uid() -> str:
    return str(uuid4())


def now() -> datetime:
    return datetime.now(UTC)


class UTCDateTime(TypeDecorator):
    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return value.astimezone(UTC) if value is not None else None

    def process_result_value(self, value, dialect):
        return value.replace(tzinfo=UTC) if value and value.tzinfo is None else value


class File(Base):
    __tablename__ = "files"
    __table_args__ = (CheckConstraint("status IN ('READY')", name="ck_file_status"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uid)
    owner_id: Mapped[str] = mapped_column(String(36), index=True)
    original_name: Mapped[str] = mapped_column(String(255))
    storage_key: Mapped[str] = mapped_column(String(255), unique=True)
    size_bytes: Mapped[int] = mapped_column(Integer)
    sha256: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), default="READY")
    last_used_at: Mapped[datetime] = mapped_column(
        UTCDateTime, default=now, index=True, nullable=True
    )
    archive_key: Mapped[str | None] = mapped_column(String(255), unique=True, index=True)
    archived_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now)


class EventArchive(Base):
    __tablename__ = "event_archives"
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), primary_key=True)
    storage_key: Mapped[str] = mapped_column(String(255), unique=True)
    sha256: Mapped[str] = mapped_column(String(64))
    event_count: Mapped[int] = mapped_column(Integer)
    last_event_id: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now)


class ComponentHeartbeat(Base):
    __tablename__ = "component_heartbeats"
    id: Mapped[str] = mapped_column(String(200), primary_key=True)
    role: Mapped[str] = mapped_column(String(24), index=True)
    mode: Mapped[str] = mapped_column(String(16))
    last_seen: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime)
    stopped_at: Mapped[datetime | None] = mapped_column(UTCDateTime)


class MaintenanceState(Base):
    __tablename__ = "maintenance_state"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    last_started_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_succeeded_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_error: Mapped[str | None] = mapped_column(String(2000))
    result_json: Mapped[dict] = mapped_column(JSON, default=dict)


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('QUEUED','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED','CANCELLED')",
            name="ck_job_status",
        ),
        CheckConstraint("attempt_count >= 0 AND max_attempts BETWEEN 1 AND 5", name="ck_attempts"),
        CheckConstraint(
            "progress IS NULL OR (progress >= 0 AND progress <= 100)", name="ck_progress"
        ),
        Index("ix_jobs_owner_created", "owner_id", "created_at"),
        Index("ix_jobs_status_next", "status", "next_run_at"),
        Index("ix_jobs_lease", "lease_until"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uid)
    owner_id: Mapped[str] = mapped_column(String(36))
    status: Mapped[str] = mapped_column(String(16), default="QUEUED")
    steps_json: Mapped[list] = mapped_column(JSON)
    csv_options: Mapped[dict] = mapped_column(JSON)
    progress: Mapped[float | None] = mapped_column(Float, nullable=True)
    phase: Mapped[str] = mapped_column(String(32), default="等待执行")
    processed_rows: Mapped[int] = mapped_column(Integer, default=0)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    next_run_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now)
    current_attempt_id: Mapped[str | None] = mapped_column(String(36))
    lease_until: Mapped[datetime | None] = mapped_column(UTCDateTime)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    result_file_id: Mapped[str | None] = mapped_column(ForeignKey("files.id"))
    parent_job_id: Mapped[str | None] = mapped_column(ForeignKey("jobs.id"))
    error_code: Mapped[str | None] = mapped_column(String(64))
    error_message: Mapped[str | None] = mapped_column(Text)
    dispatch_generation: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime)


class JobInput(Base):
    __tablename__ = "job_inputs"
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), primary_key=True)
    input_order: Mapped[int] = mapped_column(Integer, primary_key=True)
    file_id: Mapped[str] = mapped_column(ForeignKey("files.id"))


class JobAttempt(Base):
    __tablename__ = "job_attempts"
    __table_args__ = (UniqueConstraint("job_id", "attempt_no", name="uq_job_attempt"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uid)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), index=True)
    attempt_no: Mapped[int] = mapped_column(Integer)
    worker_id: Mapped[str] = mapped_column(String(128))
    queue_seconds: Mapped[float | None] = mapped_column(Float, default=0)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now)
    heartbeat_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    error_code: Mapped[str | None] = mapped_column(String(64))
    error_message: Mapped[str | None] = mapped_column(Text)


class JobEvent(Base):
    __tablename__ = "job_events"
    __table_args__ = (Index("ix_events_job_id", "job_id", "id"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"))
    attempt_id: Mapped[str | None] = mapped_column(ForeignKey("job_attempts.id"))
    event_type: Mapped[str] = mapped_column(String(32))
    detail: Mapped[str] = mapped_column(String(2000))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now)


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (UniqueConstraint("job_id", "dispatch_generation", name="uq_dispatch"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uid)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), index=True)
    dispatch_generation: Mapped[int] = mapped_column(Integer)
    sent_at: Mapped[datetime | None] = mapped_column(UTCDateTime, index=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now)


class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"
    owner_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"))
