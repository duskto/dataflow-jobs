import os
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from app.core.config import get_settings
from app.core.db import get_engine
from app.main import app


@pytest.fixture
def database(tmp_path, monkeypatch):
    if get_engine.cache_info().currsize:
        get_engine().dispose()
    get_engine.cache_clear()
    get_settings.cache_clear()
    admin = None
    if test_url := os.getenv("DF_TEST_DATABASE_URL"):
        schema = "test_" + uuid4().hex
        admin = create_engine(test_url)
        with admin.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        url = make_url(test_url).update_query_dict({"options": f"-csearch_path={schema}"})
        monkeypatch.setenv("DF_DATABASE_URL", url.render_as_string(hide_password=False))
    else:
        monkeypatch.setenv("DF_DATABASE_URL", f"sqlite:///{tmp_path / 'test.db'}")
    monkeypatch.setenv("DF_STORAGE_ROOT", str(tmp_path / "storage"))
    monkeypatch.setenv("DF_EXECUTION_MODE", "local")
    monkeypatch.setenv("DF_RETRY_BASE_SECONDS", "0")
    monkeypatch.setenv("DF_API_TOKEN", "")
    command.upgrade(Config("alembic.ini"), "head")
    yield tmp_path
    get_engine().dispose()
    get_engine.cache_clear()
    get_settings.cache_clear()
    if admin:
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


@pytest.fixture
def client(database):
    with TestClient(app) as client:
        yield client


@pytest.fixture
def uploaded(client):
    def put(content="order_id,region,amount\n1,east,0.10\n2,east,0.20\n", name="test.csv"):
        response = client.post("/api/v1/files", files={"file": (name, content)})
        assert response.status_code == 201, response.text
        return response.json()["file_id"]

    return put


@pytest.fixture
def make_job(client, uploaded):
    def create(steps=None, content=None, max_attempts=3):
        file_id = uploaded(content) if content is not None else uploaded()
        response = client.post(
            "/api/v1/jobs",
            json={
                "input_file_ids": [file_id],
                "steps": steps or [{"type": "merge"}],
                "max_attempts": max_attempts,
            },
        )
        assert response.status_code == 202, response.text
        return response.json()["id"]

    return create
