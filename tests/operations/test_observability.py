from datetime import timedelta

import pytest
from sqlalchemy import update

from app.core.config import get_settings
from app.core.db import session_factory
from app.models.entities import ComponentHeartbeat, Job, JobAttempt, now
from app.observability.heartbeat import ServiceHeartbeat
from app.observability.metrics import BUCKETS, render, snapshot
from app.scheduler.maintenance import maintain_once
from app.workers.runner import run_job


def test_heartbeats_cover_idle_processes_and_expire(client, monkeypatch):
    worker = ServiceHeartbeat("worker")
    dispatcher = ServiceHeartbeat("dispatcher")
    maintenance = ServiceHeartbeat("maintenance")
    for heartbeat in (worker, dispatcher, maintenance):
        heartbeat.pulse()
    maintain_once()
    state = snapshot(get_settings().owner_id)
    assert state["components_online"] == {"worker": 1, "dispatcher": 1, "maintenance": 1}
    assert state["alerts"] == []
    with session_factory().begin() as session:
        session.execute(
            update(ComponentHeartbeat)
            .where(ComponentHeartbeat.id == worker.id)
            .values(last_seen=now() - timedelta(minutes=2))
        )
    state = client.get("/api/v1/ops").json()
    assert state["components_online"]["worker"] == 0
    assert "NO_WORKERS" in {alert["code"] for alert in state["alerts"]}
    # Processes from a different execution mode never masquerade as live workers.
    monkeypatch.setattr(get_settings(), "execution_mode", "celery")
    assert snapshot(get_settings().owner_id, probe_dependencies=False)["components_online"] == {
        "worker": 0,
        "dispatcher": 0,
        "maintenance": 0,
    }


def test_durable_histograms_queue_age_and_owner_scope(client, make_job):
    job_id = make_job()
    with session_factory().begin() as session:
        session.execute(
            update(Job).where(Job.id == job_id).values(next_run_at=now() - timedelta(seconds=200))
        )
    queued = client.get("/api/v1/ops").json()
    assert queued["queue_oldest_seconds"] >= 200
    assert "QUEUE_DELAY" in {a["code"] for a in queued["alerts"]}
    run_job(job_id)
    with session_factory().begin() as session:
        attempt = session.query(JobAttempt).filter_by(job_id=job_id).one()
        attempt.finished_at = attempt.started_at + timedelta(seconds=4)
    state = snapshot(get_settings().owner_id)
    duration = state["attempt_duration_seconds"]
    assert duration["count"] == 1
    assert duration["sum"] == pytest.approx(4, abs=0.002)
    assert duration["buckets"]["2"] == 0 and duration["buckets"]["5"] == 1
    assert list(duration["buckets"].values()) == sorted(duration["buckets"].values())
    assert len(duration["buckets"]) == len(BUCKETS)
    assert state["queue_wait_seconds"]["sum"] >= 200
    assert state["queue_oldest_seconds"] == 0
    assert snapshot("another-owner")["attempt_duration_seconds"]["count"] == 0
    metrics = render(state)
    assert 'dataflow_attempt_duration_seconds_bucket{le="+Inf"} 1' in metrics
    assert 'dataflow_jobs{status="FAILED"} 0' in metrics
    assert client.get("/api/v1/metrics").status_code == 200


def test_failure_alert_has_minimum_sample_size(client, make_job, monkeypatch):
    monkeypatch.setattr(get_settings(), "alert_failure_min_jobs", 2)
    first = make_job(content="bad,header\nwrong\n")
    run_job(first)
    assert "FAILURE_RATE" not in {a["code"] for a in snapshot(get_settings().owner_id)["alerts"]}
    second = make_job(content="bad,header\nwrong\n")
    run_job(second)
    result = snapshot(get_settings().owner_id)
    assert result["recent_failure_rate"] == 1
    assert "FAILURE_RATE" in {a["code"] for a in result["alerts"]}


def test_operations_endpoint_requires_configured_token(client, monkeypatch):
    monkeypatch.setattr(get_settings(), "api_token", "secret")
    for endpoint in ("ops", "metrics"):
        assert client.get(f"/api/v1/{endpoint}").status_code == 401
        assert (
            client.get(
                f"/api/v1/{endpoint}", headers={"Authorization": "Bearer secret"}
            ).status_code
            == 200
        )


def test_upgrade_preserves_existing_uploads_and_backfills_last_use(database):
    from uuid import uuid4

    from alembic import command
    from alembic.config import Config
    from sqlalchemy import text

    from app.core.db import get_engine
    from app.models.entities import File

    config = Config("alembic.ini")
    command.downgrade(config, "eaddd8464820")
    file_id, timestamp = str(uuid4()), now() - timedelta(days=2)
    with get_engine().begin() as connection:
        connection.execute(
            text(
                "INSERT INTO files "
                "(id, owner_id, original_name, storage_key, size_bytes, "
                "sha256, status, created_at) "
                "VALUES (:id, :owner, 'old.csv', 'uploads/old.csv', 5, :sha, 'READY', :created)"
            ),
            {
                "id": file_id,
                "owner": get_settings().owner_id,
                "sha": "a" * 64,
                "created": timestamp.isoformat(),
            },
        )
    command.upgrade(config, "head")
    command.check(config)
    with session_factory()() as session:
        file = session.get(File, file_id)
        assert file.last_used_at == timestamp
        assert file.archive_key is None and file.storage_key == "uploads/old.csv"
