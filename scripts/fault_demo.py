"""Real Compose fault experiment. Operates only this project's worker/redis services."""

import argparse
import subprocess
import time
from pathlib import Path

import httpx


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def wait(client, job_id, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get(f"/jobs/{job_id}")
        response.raise_for_status()
        job = response.json()
        if job["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            assert job["status"] == "SUCCEEDED", job
            return job
        time.sleep(0.2)
    raise TimeoutError(job_id)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--input", type=Path, default=Path("data/benchmark.csv"))
    args = parser.parse_args()
    import os

    headers = (
        {"Authorization": f"Bearer {os.environ['DF_API_TOKEN']}"}
        if os.getenv("DF_API_TOKEN")
        else {}
    )
    with httpx.Client(
        base_url=args.url + "/api/v1", trust_env=False, timeout=30, headers=headers
    ) as client:
        with args.input.open("rb") as stream:
            response = client.post("/files", files={"file": ("fault.csv", stream)})
        response.raise_for_status()
        body = {
            "input_file_ids": [response.json()["file_id"]],
            "steps": [
                {"type": "merge"},
                {"type": "deduplicate", "keys": ["order_id"]},
                {"type": "aggregate", "group_by": ["region"], "sum": ["amount"]},
            ],
        }
        try:
            compose("stop", "redis")
            response = client.post("/jobs", json=body)
            response.raise_for_status()
            queued_id = response.json()["id"]
            time.sleep(2)
            assert client.get(f"/jobs/{queued_id}").json()["status"] == "QUEUED"
        finally:
            compose("start", "redis")
        baseline = wait(client, queued_id)
        expected = client.get(f"/jobs/{queued_id}/result").content
        print(
            "PASS: Redis outage → durable outbox → SUCCEEDED "
            f"({baseline['attempt_count']} attempt)",
            flush=True,
        )

        response = client.post("/jobs", json=body)
        response.raise_for_status()
        job_id = response.json()["id"]
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            state = client.get(f"/jobs/{job_id}").json()["status"]
            if state == "RUNNING":
                break
            if state in {"SUCCEEDED", "FAILED"}:
                raise RuntimeError("Job finished before kill; use a larger input")
            time.sleep(0.01)
        else:
            raise TimeoutError("Job never started")
        try:
            compose("kill", "-s", "SIGKILL", "worker")
        finally:
            compose("start", "worker")
        job = wait(client, job_id)
        assert job["attempt_count"] >= 2, job
        assert client.get(f"/jobs/{job_id}/result").content == expected
        print(
            "PASS: SIGKILL worker → lease recovery → identical result "
            f"({job['attempt_count']} attempts)"
        )


if __name__ == "__main__":
    main()
