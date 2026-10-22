from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from alembic import command
from alembic.config import Config
from sqlalchemy import func, select

from app.core.config import get_settings
from app.core.db import session_factory
from app.models.entities import File, Job, Outbox
from app.workers.runner import run_job


def test_full_api_workflow(client, uploaded):
    ids = [
        uploaded("order_id,region,amount\n1,east,0.10\n"),
        uploaded("order_id,region,amount\n1,east,99\n2,east,0.20\n"),
    ]
    body = {
        "input_file_ids": ids,
        "steps": [
            {"type": "merge"},
            {"type": "deduplicate", "keys": ["order_id"]},
            {"type": "aggregate", "group_by": ["region"], "sum": ["amount"]},
        ],
    }
    response = client.post("/api/v1/jobs", json=body, headers={"Idempotency-Key": "demo"})
    assert response.status_code == 202
    job = response.json()
    assert job["created_at"].endswith("Z") or job["created_at"].endswith("+00:00")
    assert (
        client.post("/api/v1/jobs", json=body, headers={"Idempotency-Key": "demo"}).json()["id"]
        == job["id"]
    )
    assert client.get(f"/api/v1/jobs/{job['id']}/result").status_code == 409
    run_job(job["id"])
    result = client.get(f"/api/v1/jobs/{job['id']}/result")
    assert result.status_code == 200
    assert result.content.decode("utf-8-sig") == "region,amount\r\neast,0.30\r\n"
    detail = client.get(f"/api/v1/jobs/{job['id']}").json()
    assert detail["status"] == "SUCCEEDED" and len(detail["attempts"]) == 1
    events = client.get(f"/api/v1/jobs/{job['id']}/events?limit=1").json()
    assert events["has_more"]
    assert client.get(f"/api/v1/jobs/{job['id']}/events?after={events['next_cursor']}").json()[
        "items"
    ]
    assert client.get("/api/v1/health/ready").status_code == 200
    assert "SUCCEEDED" in client.get("/api/v1/metrics").text
    assert client.get("/").status_code == 200


def test_idempotency_conflict_and_concurrent_create(client, uploaded):
    file_id = uploaded()
    body = {"input_file_ids": [file_id], "steps": [{"type": "merge"}]}

    def submit(_):
        return client.post("/api/v1/jobs", json=body, headers={"Idempotency-Key": "race"})

    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(pool.map(submit, range(4)))
    assert all(r.status_code == 202 for r in responses)
    assert len({r.json()["id"] for r in responses}) == 1
    body["max_attempts"] = 1
    assert submit(None).status_code == 409
    with session_factory()() as session:
        assert session.scalar(select(func.count()).select_from(Job)) == 1
        assert session.scalar(select(func.count()).select_from(Outbox)) == 1


def test_validation_and_uniform_errors(client, uploaded):
    file_id = uploaded()
    for steps in [
        [{"type": "shell", "command": "ls"}],
        [{"type": "merge"}, {"type": "merge"}],
        [{"type": "deduplicate", "keys": ["x", "x"]}],
        [{"type": "aggregate", "group_by": ["x"], "sum": ["x"]}],
    ]:
        response = client.post("/api/v1/jobs", json={"input_file_ids": [file_id], "steps": steps})
        assert response.status_code == 422
        assert set(response.json()) == {"code", "message", "request_id"}
    assert client.get("/api/v1/jobs?limit=1000").status_code == 422
    assert client.get("/api/v1/jobs?status=NOPE").status_code == 422
    assert client.get("/api/v1/jobs/not-a-uuid").status_code == 422
    assert client.get(f"/api/v1/files/{uuid4()}").status_code == 404


def test_upload_limits_and_names(client, monkeypatch):
    monkeypatch.setattr(get_settings(), "max_upload_bytes", 10)
    response = client.post("/api/v1/files", files={"file": ("x.csv", b"a" * 11)})
    assert response.status_code == 413
    assert not list(get_settings().storage_root.rglob("*.tmp"))
    assert client.post("/api/v1/files", files={"file": ("x.csv", b"")}).status_code == 422
    assert client.post("/api/v1/files", files={"file": ("x.exe", b"a")}).status_code == 422
    response = client.post("/api/v1/files", files={"file": ("../../etc/x.csv", b"a\n")})
    assert response.status_code == 201
    assert response.json()["original_name"] == "x.csv"


def test_owner_isolation_and_optional_auth(client, uploaded, make_job, monkeypatch):
    file_id, job_id = uploaded(), make_job()
    monkeypatch.setattr(get_settings(), "owner_id", str(uuid4()))
    assert client.get(f"/api/v1/files/{file_id}").status_code == 404
    for suffix in ("", "/events", "/result"):
        assert client.get(f"/api/v1/jobs/{job_id}{suffix}").status_code == 404
    for suffix in ("cancel", "retry"):
        assert client.post(f"/api/v1/jobs/{job_id}/{suffix}").status_code == 404
    assert client.get("/api/v1/jobs").json()["items"] == []
    assert (
        client.post(
            "/api/v1/jobs", json={"input_file_ids": [file_id], "steps": [{"type": "merge"}]}
        ).status_code
        == 404
    )
    monkeypatch.setattr(get_settings(), "api_token", "test-secret")
    assert client.get("/api/v1/jobs").status_code == 401
    assert (
        client.get("/api/v1/jobs", headers={"Authorization": "Bearer test-secret"}).status_code
        == 200
    )


def test_failure_is_not_automatically_retried_and_manual_retry_keeps_parent(client, make_job):
    job_id = make_job(
        steps=[{"type": "aggregate", "group_by": ["region"], "sum": ["amount"]}],
        content="region,amount\neast,invalid\n",
    )
    run_job(job_id)
    job = client.get(f"/api/v1/jobs/{job_id}").json()
    assert job["status"] == "FAILED" and job["error_code"] == "INVALID_NUMBER"
    assert job["attempt_count"] == 1
    retried = client.post(f"/api/v1/jobs/{job_id}/retry").json()
    assert retried["parent_job_id"] == job_id and retried["id"] != job_id
    assert client.post(f"/api/v1/jobs/{retried['id']}/retry").status_code == 409


def test_cancel_queued_and_success_is_terminal(client, make_job):
    job_id = make_job()
    assert client.post(f"/api/v1/jobs/{job_id}/cancel").json()["status"] == "CANCELLED"
    run_job(job_id)
    assert client.get(f"/api/v1/jobs/{job_id}").json()["attempt_count"] == 0
    job_id = make_job()
    run_job(job_id)
    assert client.post(f"/api/v1/jobs/{job_id}/cancel").json()["status"] == "SUCCEEDED"


def test_migration_roundtrip(database):
    config = Config("alembic.ini")
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    command.check(config)
    with session_factory()() as session:
        assert session.scalar(select(func.count()).select_from(File)) == 0


def test_multipart_body_is_bounded_before_spooling(client, monkeypatch):
    monkeypatch.setattr(get_settings(), "max_upload_bytes", 10)
    response = client.post("/api/v1/files", files={"file": ("big.csv", b"x" * 1100000)})
    assert response.status_code == 413
    assert response.json()["code"] == "FILE_TOO_LARGE"
    assert response.json()["request_id"] == response.headers["X-Request-ID"]


def test_streaming_multipart_size_limit_without_content_length(client, monkeypatch):
    monkeypatch.setattr(get_settings(), "max_upload_bytes", 10)

    def chunks():
        yield b'--boundary\r\nContent-Disposition: form-data; name="file"; filename="a.csv"\r\n\r\n'
        for _ in range(20):
            yield b"x" * 65536
        yield b"\r\n--boundary--\r\n"

    response = client.post(
        "/api/v1/files",
        content=chunks(),
        headers={"Content-Type": "multipart/form-data; boundary=boundary"},
    )
    assert response.status_code == 413
    assert response.json()["code"] == "FILE_TOO_LARGE"
