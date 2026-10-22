"""Shared, durable metrics derived from authoritative DB records, not API process memory."""

import time
from datetime import timedelta

from sqlalchemy import case, extract, func, select

from app.core.config import get_settings
from app.core.db import get_engine, session_factory
from app.models.entities import (
    ComponentHeartbeat,
    EventArchive,
    File,
    Job,
    JobAttempt,
    MaintenanceState,
    Outbox,
    now,
)

STATUSES = ("QUEUED", "RUNNING", "RETRY_WAIT", "SUCCEEDED", "FAILED", "CANCELLED")
BUCKETS = (0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600)


def histogram(session, expression, conditions) -> dict:
    row = session.execute(
        select(
            func.count(),
            func.coalesce(func.sum(expression), 0),
            *[
                func.coalesce(func.sum(case((expression <= bound, 1), else_=0)), 0)
                for bound in BUCKETS
            ],
        )
        .select_from(JobAttempt)
        .join(Job, Job.id == JobAttempt.job_id)
        .where(*conditions)
    ).one()
    return {
        "count": row[0],
        "sum": float(row[1]),
        "buckets": {str(bound): row[i + 2] for i, bound in enumerate(BUCKETS)},
    }


def snapshot(owner: str, *, probe_dependencies: bool = True) -> dict:
    settings, current = get_settings(), now()
    cutoff = current - timedelta(seconds=settings.heartbeat_timeout_seconds)
    with session_factory()() as session:
        counts = dict(
            session.execute(
                select(Job.status, func.count()).where(Job.owner_id == owner).group_by(Job.status)
            ).all()
        )
        counts = {status: counts.get(status, 0) for status in STATUSES}
        oldest = session.scalar(
            select(func.min(Job.next_run_at)).where(
                Job.owner_id == owner, Job.status == "QUEUED", Job.next_run_at <= current
            )
        )
        pending = session.scalar(
            select(func.count())
            .select_from(Outbox)
            .join(Job)
            .where(Job.owner_id == owner, Job.status == "QUEUED", Outbox.sent_at.is_(None))
        )
        expired = session.scalar(
            select(func.count())
            .select_from(Job)
            .where(Job.owner_id == owner, Job.status == "RUNNING", Job.lease_until <= current)
        )
        online = dict(
            session.execute(
                select(ComponentHeartbeat.role, func.count())
                .where(
                    ComponentHeartbeat.mode == settings.execution_mode,
                    ComponentHeartbeat.last_seen >= cutoff,
                    ComponentHeartbeat.stopped_at.is_(None),
                )
                .group_by(ComponentHeartbeat.role)
            ).all()
        )
        # This aggregates bounded SQL result rows. It does not load all attempts into Python.
        if get_engine().dialect.name == "sqlite":
            raw_duration = (
                func.julianday(JobAttempt.finished_at) - func.julianday(JobAttempt.started_at)
            ) * 86400
        else:
            raw_duration = extract("epoch", JobAttempt.finished_at - JobAttempt.started_at)
        duration = case((raw_duration < 0, 0), else_=raw_duration)
        timings = histogram(
            session, duration, [Job.owner_id == owner, JobAttempt.finished_at.is_not(None)]
        )
        queue = histogram(
            session,
            JobAttempt.queue_seconds,
            [Job.owner_id == owner, JobAttempt.queue_seconds.is_not(None)],
        )
        recent = dict(
            session.execute(
                select(Job.status, func.count())
                .where(
                    Job.owner_id == owner,
                    Job.finished_at >= current - timedelta(hours=1),
                    Job.status.in_(["SUCCEEDED", "FAILED"]),
                )
                .group_by(Job.status)
            ).all()
        )
        finished = sum(recent.values())
        failed = recent.get("FAILED", 0)
        maintenance = session.get(MaintenanceState, "retention")
        state = (
            {
                "last_started_at": maintenance.last_started_at,
                "last_succeeded_at": maintenance.last_succeeded_at,
                "last_error": maintenance.last_error,
                "result": maintenance.result_json,
            }
            if maintenance
            else None
        )
        inputs_archived = session.scalar(
            select(func.count())
            .select_from(File)
            .where(File.owner_id == owner, File.archive_key.is_not(None))
        )
        events_archived = session.scalar(
            select(func.coalesce(func.sum(EventArchive.event_count), 0))
            .join(Job)
            .where(Job.owner_id == owner)
        )
    broker_up = True
    if settings.execution_mode == "celery" and probe_dependencies:
        from redis import Redis

        try:
            with Redis.from_url(
                settings.redis_url, socket_connect_timeout=1, socket_timeout=1
            ) as r:
                broker_up = bool(r.ping())
        except Exception:
            broker_up = False
    age = max(0, (current - oldest).total_seconds()) if oldest else 0
    online = {role: online.get(role, 0) for role in ("worker", "dispatcher", "maintenance")}
    alerts = []

    def alert(code, condition, message):
        if condition:
            alerts.append({"code": code, "message": message})

    alert("NO_WORKERS", online["worker"] == 0, "没有在线 Worker")
    alert("NO_DISPATCHER", online["dispatcher"] == 0, "派发与恢复进程离线")
    alert("NO_MAINTENANCE", online["maintenance"] == 0, "定期清理与归档进程离线")
    alert("BROKER_UNAVAILABLE", not broker_up, "Redis 暂时不可用，任务将保留在 outbox")
    alert("QUEUE_DELAY", age > settings.alert_queue_age_seconds, "最早排队任务超过等待阈值")
    alert("LEASE_EXPIRED", expired > 0, "存在租约过期、等待恢复的任务")
    alert(
        "FAILURE_RATE",
        finished >= settings.alert_failure_min_jobs
        and failed / max(finished, 1) > settings.alert_failure_rate,
        "近一小时任务失败率超过阈值",
    )
    success_time = state["last_succeeded_at"] if state else None
    overdue = not success_time or (current - success_time).total_seconds() > (
        settings.maintenance_interval_seconds * 2 + settings.heartbeat_timeout_seconds
    )
    alert("MAINTENANCE_FAILED", bool(state and state["last_error"]), "最近清理或归档执行失败")
    alert("MAINTENANCE_OVERDUE", overdue, "清理与归档尚未成功完成，或已超过执行间隔")
    return {
        "collected_at": current,
        "mode": settings.execution_mode,
        "jobs": counts,
        "queue_oldest_seconds": age,
        "outbox_pending": pending,
        "expired_leases": expired,
        "components_online": online,
        "broker_up": broker_up,
        "attempt_duration_seconds": timings,
        "queue_wait_seconds": queue,
        "recent_finished": finished,
        "recent_failed": failed,
        "recent_failure_rate": failed / max(finished, 1),
        "maintenance": state,
        "inputs_archived": inputs_archived,
        "events_archived": events_archived,
        "alerts": alerts,
    }


def render(data: dict) -> str:
    lines = []

    def metric(name, kind, help_text, samples):
        lines.extend([f"# HELP {name} {help_text}", f"# TYPE {name} {kind}"])
        lines.extend(f"{name}{labels} {value}" for labels, value in samples)

    metric(
        "dataflow_jobs",
        "gauge",
        "Current jobs by status",
        [(f'{{status="{status}"}}', count) for status, count in data["jobs"].items()],
    )
    for key in (
        "queue_oldest_seconds",
        "outbox_pending",
        "expired_leases",
        "recent_finished",
        "recent_failed",
        "recent_failure_rate",
        "inputs_archived",
        "events_archived",
    ):
        metric(f"dataflow_{key}", "gauge", key.replace("_", " "), [("", data[key])])
    metric(
        "dataflow_components_online",
        "gauge",
        "Fresh process heartbeats by role",
        [(f'{{role="{role}"}}', value) for role, value in data["components_online"].items()],
    )
    metric(
        "dataflow_broker_up",
        "gauge",
        "Broker reachable (1 for local mode)",
        [("", int(data["broker_up"]))],
    )
    state = data["maintenance"]
    succeeded = (
        state["last_succeeded_at"].timestamp() if state and state["last_succeeded_at"] else 0
    )
    metric(
        "dataflow_maintenance_last_success_timestamp_seconds",
        "gauge",
        "Last successful maintenance cycle UTC timestamp",
        [("", succeeded)],
    )
    codes = (
        "NO_WORKERS",
        "NO_DISPATCHER",
        "NO_MAINTENANCE",
        "BROKER_UNAVAILABLE",
        "QUEUE_DELAY",
        "LEASE_EXPIRED",
        "FAILURE_RATE",
        "MAINTENANCE_FAILED",
        "MAINTENANCE_OVERDUE",
    )
    active = {a["code"] for a in data["alerts"]}
    metric(
        "dataflow_alert_active",
        "gauge",
        "Application alert condition (before Prometheus for)",
        [(f'{{code="{code}"}}', int(code in active)) for code in codes],
    )
    for key in ("attempt_duration_seconds", "queue_wait_seconds"):
        name, h = f"dataflow_{key}", data[key]
        lines.extend(
            [f"# HELP {name} Cumulative recorded attempt observations", f"# TYPE {name} histogram"]
        )
        lines.extend(
            f'{name}_bucket{{le="{bound}"}} {value}' for bound, value in h["buckets"].items()
        )
        lines.extend(
            [
                f'{name}_bucket{{le="+Inf"}} {h["count"]}',
                f"{name}_count {h['count']}",
                f"{name}_sum {h['sum']}",
            ]
        )
    metric("dataflow_metrics_timestamp_seconds", "gauge", "Collection time", [("", time.time())])
    return "\n".join(lines) + "\n"
