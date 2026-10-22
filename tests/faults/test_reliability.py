import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.exc import OperationalError

from app.core.db import session_factory
from app.core.errors import Cancelled, LeaseLost
from app.models.entities import File, Job, JobAttempt, Outbox, now
from app.scheduler.dispatcher import dispatch_once
from app.scheduler.recovery import recover
from app.services.jobs import claim, finish_failure, finish_success, heartbeat
from app.workers.runner import run_job
from scripts.cleanup import cleanup


def expire(job_id):
    with session_factory().begin() as session:
        session.execute(
            update(Job).where(Job.id == job_id).values(lease_until=now() - timedelta(seconds=1))
        )


def test_duplicate_delivery_publishes_once(client, make_job):
    job_id = make_job()
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(run_job, [job_id] * 4))
    with session_factory()() as session:
        assert session.get(Job, job_id).status == "SUCCEEDED"
        assert session.scalar(select(func.count()).select_from(JobAttempt)) == 1
        assert session.scalar(select(func.count()).select_from(File)) == 2


def test_expired_worker_fenced_after_recovery(client, make_job):
    job_id = make_job()
    old_attempt, _ = claim(job_id, "old")
    expire(job_id)
    assert recover() == 1
    new_attempt, _ = claim(job_id, "new")
    assert new_attempt != old_attempt
    with pytest.raises(LeaseLost):
        heartbeat(job_id, old_attempt, "old", 999)
    assert not finish_success(job_id, old_attempt, "old.csv", 0, "0" * 64)
    finish_failure(job_id, old_attempt, "OLD_FAILURE", "stale")
    assert client.get(f"/api/v1/jobs/{job_id}").json()["status"] == "RUNNING"
    assert finish_success(job_id, new_attempt, "new.csv", 0, "1" * 64)
    assert not finish_success(job_id, old_attempt, "old.csv", 0, "0" * 64)


def test_expired_lease_cannot_commit_even_before_recovery(client, make_job):
    job_id = make_job()
    attempt, _ = claim(job_id, "old")
    expire(job_id)
    assert not finish_success(job_id, attempt, "bad.csv", 0, "0" * 64)
    with pytest.raises(LeaseLost):
        heartbeat(job_id, attempt, "late", 0)


def test_crash_at_max_attempts_fails(client, make_job):
    job_id = make_job(max_attempts=1)
    claim(job_id, "dead-worker")
    expire(job_id)
    recover()
    job = client.get(f"/api/v1/jobs/{job_id}").json()
    assert job["status"] == "FAILED" and job["error_code"] == "LEASE_EXPIRED"


def test_cancel_wins_before_result_commit(client, make_job):
    job_id = make_job()
    attempt, _ = claim(job_id, "worker")
    response = client.post(f"/api/v1/jobs/{job_id}/cancel").json()
    assert response["status"] == "RUNNING" and response["cancel_requested"]
    assert not finish_success(job_id, attempt, "bad.csv", 0, "0" * 64)
    with pytest.raises(Cancelled):
        heartbeat(job_id, attempt, "写出", 2)
    finish_failure(job_id, attempt, "CANCELLED", "cancelled")
    assert client.get(f"/api/v1/jobs/{job_id}").json()["status"] == "CANCELLED"
    assert client.get(f"/api/v1/jobs/{job_id}/result").status_code == 409


def test_cancel_dead_worker_recovers_to_cancelled(client, make_job):
    job_id = make_job()
    claim(job_id, "dead")
    client.post(f"/api/v1/jobs/{job_id}/cancel")
    expire(job_id)
    recover()
    assert client.get(f"/api/v1/jobs/{job_id}").json()["status"] == "CANCELLED"


def test_outbox_survives_broker_failure_and_duplicate_send(client, make_job):
    job_id = make_job()

    def unavailable(_):
        raise ConnectionError("redis offline")

    assert dispatch_once(unavailable) == 0
    with session_factory()() as session:
        assert session.scalar(select(Outbox)).sent_at is None
    delivered = []
    assert dispatch_once(delivered.append) == 1
    assert delivered == [job_id]
    assert dispatch_once(delivered.append) == 0
    # Simulate accepted-but-lost broker message; stale queued jobs are re-dispatched.
    with session_factory().begin() as session:
        session.execute(update(Outbox).values(sent_at=now() - timedelta(minutes=5)))
    recover()
    assert dispatch_once(delivered.append) == 1
    for job_id in delivered:
        run_job(job_id)
    assert client.get(f"/api/v1/jobs/{job_id}").json()["attempt_count"] == 1


def test_transient_storage_failure_retry(client, make_job):
    job_id = make_job()
    with patch("app.workers.runner.process", side_effect=OSError("temporary failure")):
        run_job(job_id)
    assert client.get(f"/api/v1/jobs/{job_id}").json()["status"] == "RETRY_WAIT"
    recover()
    run_job(job_id)
    job = client.get(f"/api/v1/jobs/{job_id}").json()
    assert job["status"] == "SUCCEEDED" and job["attempt_count"] == 2


def test_db_failure_after_file_write_never_reports_success(client, make_job):
    job_id = make_job()
    with patch(
        "app.workers.runner.finish_success", side_effect=OperationalError("", {}, Exception())
    ):
        run_job(job_id)
    assert client.get(f"/api/v1/jobs/{job_id}").json()["status"] == "RETRY_WAIT"
    assert client.get(f"/api/v1/jobs/{job_id}/result").status_code == 409
    from app.storage.local import LocalStorage

    orphan = next((LocalStorage().root / "jobs").rglob("*.csv"))
    os.utime(orphan, (time.time() - 90000,) * 2)
    assert cleanup() == [orphan.relative_to(LocalStorage().root).as_posix()]
    assert orphan.exists()
    cleanup(apply=True)
    assert not orphan.exists()
    recover()
    run_job(job_id)
    assert client.get(f"/api/v1/jobs/{job_id}").json()["status"] == "SUCCEEDED"
    # Referenced results are never garbage-collected.
    result = next((LocalStorage().root / "jobs").rglob("*.csv"))
    os.utime(result, (time.time() - 90000,) * 2)
    assert cleanup(apply=True) == []
    assert result.exists()


def test_independent_heartbeat_keeps_blocked_processor_alive(client, make_job, monkeypatch):
    import threading

    from app.core.config import get_settings
    from app.workers import runner

    monkeypatch.setattr(get_settings(), "lease_seconds", 3)
    job_id = make_job()
    started, release = threading.Event(), threading.Event()
    original = runner.process

    def blocked(*args, **kwargs):
        started.set()
        assert release.wait(10)
        return original(*args, **kwargs)

    with patch("app.workers.runner.process", side_effect=blocked):
        with ThreadPoolExecutor(max_workers=1) as pool:
            running = pool.submit(run_job, job_id)
            try:
                assert started.wait(3)
                time.sleep(3.3)
                assert recover() == 0
                assert client.get(f"/api/v1/jobs/{job_id}").json()["status"] == "RUNNING"
            finally:
                release.set()
            running.result(timeout=10)
    assert client.get(f"/api/v1/jobs/{job_id}").json()["status"] == "SUCCEEDED"


def test_retry_wait_does_not_allow_early_claim_and_can_be_cancelled(client, make_job, monkeypatch):
    from app.core.config import get_settings

    monkeypatch.setattr(get_settings(), "retry_base_seconds", 60)
    job_id = make_job()
    attempt, _ = claim(job_id, "worker")
    finish_failure(job_id, attempt, "STORAGE_UNAVAILABLE", "temporary", retryable=True)
    recover()
    assert claim(job_id, "too-early") is None
    assert client.get(f"/api/v1/jobs/{job_id}").json()["status"] == "RETRY_WAIT"
    assert client.post(f"/api/v1/jobs/{job_id}/cancel").json()["status"] == "CANCELLED"
