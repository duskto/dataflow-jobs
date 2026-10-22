import gzip
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import func, select, update

from app.core.config import get_settings
from app.core.db import session_factory
from app.models.entities import EventArchive, File, Job, JobEvent, MaintenanceState, now
from app.scheduler.maintenance import maintain_once
from app.services.retention import archive_batch, archive_events, archive_input
from app.storage.local import LocalStorage
from app.workers.runner import run_job
from scripts.cleanup import cleanup


def age_file(file_id):
    with session_factory().begin() as session:
        session.execute(
            update(File).where(File.id == file_id).values(last_used_at=now() - timedelta(days=40))
        )
    return now() - timedelta(days=30)


def test_archived_input_is_transparently_reusable(client, uploaded):
    file_id = uploaded()
    cutoff = age_file(file_id)
    with session_factory()() as session:
        original = LocalStorage().path(session.get(File, file_id).storage_key)
    expected = original.read_bytes()
    assert archive_input(file_id, cutoff)
    assert not original.exists()
    assert not archive_input(file_id, cutoff)
    assert client.get(f"/api/v1/files/{file_id}").json()["archived_at"]
    with session_factory()() as session:
        archive = LocalStorage().path(session.get(File, file_id).archive_key)
    assert gzip.decompress(archive.read_bytes()) == expected
    os.utime(archive, (time.time() - 90000,) * 2)
    assert cleanup(apply=True) == []
    response = client.post(
        "/api/v1/jobs", json={"input_file_ids": [file_id], "steps": [{"type": "merge"}]}
    )
    job_id = response.json()["id"]
    run_job(job_id)
    assert client.get(f"/api/v1/jobs/{job_id}").json()["status"] == "SUCCEEDED"
    result = client.get(f"/api/v1/jobs/{job_id}/result").content.decode("utf-8-sig")
    assert result.replace("\r\n", "\n") == expected.decode()
    assert not list((LocalStorage().root / "jobs").rglob("input-*.csv"))


def test_archive_excludes_active_inputs_and_new_submission_wins(client, uploaded):
    from app.services import retention

    file_id = uploaded()
    cutoff = age_file(file_id)
    original_writer = retention.write_gzip
    new_job = None

    def create_during_archive(*args, **kwargs):
        nonlocal new_job
        output = original_writer(*args, **kwargs)
        new_job = client.post(
            "/api/v1/jobs", json={"input_file_ids": [file_id], "steps": [{"type": "merge"}]}
        ).json()["id"]
        return output

    with patch("app.services.retention.write_gzip", side_effect=create_during_archive):
        assert not archive_input(file_id, cutoff)
    # Even if its last-used timestamp is old, an active job protects its input.
    age_file(file_id)
    assert archive_batch()["inputs_archived"] == 0
    run_job(new_job)
    assert client.get(f"/api/v1/jobs/{new_job}").json()["status"] == "SUCCEEDED"
    assert archive_input(file_id, cutoff)


def test_bad_input_checksum_never_removes_original(client, uploaded):
    file_id = uploaded()
    cutoff = age_file(file_id)
    with session_factory()() as session:
        original = LocalStorage().path(session.get(File, file_id).storage_key)
    original.write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="integrity"):
        archive_input(file_id, cutoff)
    assert original.exists()
    with session_factory()() as session:
        assert session.get(File, file_id).archive_key is None
    assert not list(LocalStorage().root.rglob("*.tmp"))


def test_corrupt_archive_fails_without_publishing(client, uploaded):
    file_id = uploaded()
    assert archive_input(file_id, age_file(file_id))
    with session_factory()() as session:
        LocalStorage().path(session.get(File, file_id).archive_key).write_bytes(b"broken gzip")
    job = client.post(
        "/api/v1/jobs", json={"input_file_ids": [file_id], "steps": [{"type": "merge"}]}
    ).json()
    run_job(job["id"])
    info = client.get(f"/api/v1/jobs/{job['id']}").json()
    assert info["status"] == "FAILED" and info["error_code"] == "ARCHIVE_CORRUPT"
    assert not list((LocalStorage().root / "jobs").rglob("input-*.csv"))


def test_events_archived_atomically_and_download_is_owner_scoped(client, make_job, monkeypatch):
    job_id = make_job()
    run_job(job_id)
    expected = client.get(f"/api/v1/jobs/{job_id}/events").json()["items"]
    with session_factory().begin() as session:
        session.execute(
            update(Job).where(Job.id == job_id).values(finished_at=now() - timedelta(days=40))
        )
    cutoff = now() - timedelta(days=30)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: archive_events(job_id, cutoff), range(2)))
    assert sum(results) == 1
    response = client.get(f"/api/v1/jobs/{job_id}/events")
    assert response.json()["items"] == []
    assert response.json()["archive"]["event_count"] == len(expected)
    downloaded = client.get(f"/api/v1/jobs/{job_id}/events/archive")
    assert downloaded.status_code == 200
    archived = [json.loads(line) for line in gzip.decompress(downloaded.content).splitlines()]
    assert [e["id"] for e in archived] == [e["id"] for e in expected]
    assert [e["detail"] for e in archived] == [e["detail"] for e in expected]
    with session_factory()() as session:
        assert session.scalar(select(func.count()).select_from(JobEvent)) == 0
        path = LocalStorage().path(session.get(EventArchive, job_id).storage_key)
    os.utime(path, (time.time() - 90000,) * 2)
    cleanup(apply=True)
    assert path.exists()
    monkeypatch.setattr(get_settings(), "owner_id", "different-owner")
    assert client.get(f"/api/v1/jobs/{job_id}/events/archive").status_code == 404


def test_periodic_maintenance_records_success_failure_and_recovers(client, uploaded):
    file_id = uploaded()
    age_file(file_id)
    orphan = LocalStorage().path("uploads/unreferenced.csv")
    orphan.write_bytes(b"orphan")
    os.utime(orphan, (time.time() - 90000,) * 2)
    result = maintain_once()
    assert result["inputs_archived"] == 1 and result["orphans_removed"] == 1
    with session_factory()() as session:
        succeeded = session.get(MaintenanceState, "retention").last_succeeded_at
    with patch("app.scheduler.maintenance.archive_batch", side_effect=OSError("disk offline")):
        with pytest.raises(OSError):
            maintain_once()
    with session_factory()() as session:
        state = session.get(MaintenanceState, "retention")
        assert state.last_succeeded_at == succeeded
        assert "disk offline" in state.last_error
    maintain_once()
    with session_factory()() as session:
        assert session.get(MaintenanceState, "retention").last_error is None


def test_downgrade_refuses_to_discard_archive_references(client, uploaded):
    from alembic import command
    from alembic.config import Config

    file_id = uploaded()
    archive_input(file_id, age_file(file_id))
    with pytest.raises(RuntimeError, match="Restore archived inputs"):
        command.downgrade(Config("alembic.ini"), "eaddd8464820")
    with session_factory()() as session:
        assert session.get(File, file_id).archive_key


def test_archive_metadata_commit_failure_keeps_source_readable(client, uploaded):
    from sqlalchemy import event

    from app.core.db import get_engine

    file_id = uploaded()
    cutoff = age_file(file_id)
    with session_factory()() as session:
        original = LocalStorage().path(session.get(File, file_id).storage_key)
    expected = original.read_bytes()

    def fail_publish(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE files SET archive_key"):
            raise OSError("archive metadata commit unavailable")

    event.listen(get_engine(), "before_cursor_execute", fail_publish)
    try:
        with pytest.raises(OSError, match="metadata commit"):
            archive_input(file_id, cutoff)
    finally:
        event.remove(get_engine(), "before_cursor_execute", fail_publish)
    assert original.read_bytes() == expected
    with session_factory()() as session:
        assert session.get(File, file_id).archive_key is None
    assert len(list((LocalStorage().root / "archives").rglob("*.gz"))) == 1
