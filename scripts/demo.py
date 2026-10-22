"""Exercise an actual HTTP server and compare its asynchronous result to the sample."""

import argparse
import csv
import io
import os
import time
from pathlib import Path
from uuid import uuid4

import httpx


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    headers = (
        {"Authorization": f"Bearer {os.environ['DF_API_TOKEN']}"}
        if os.getenv("DF_API_TOKEN")
        else {}
    )
    with httpx.Client(
        base_url=args.url + "/api/v1", headers=headers, timeout=30, trust_env=False
    ) as client:
        ids = []
        for name in ("january.csv", "february.csv"):
            with Path("samples", name).open("rb") as stream:
                response = client.post("/files", files={"file": (name, stream, "text/csv")})
            response.raise_for_status()
            ids.append(response.json()["file_id"])
        body = {
            "input_file_ids": ids,
            "steps": [
                {"type": "merge"},
                {"type": "deduplicate", "keys": ["order_id"]},
                {"type": "aggregate", "group_by": ["region"], "sum": ["amount"]},
            ],
        }
        key = str(uuid4())
        response = client.post("/jobs", json=body, headers={"Idempotency-Key": key})
        response.raise_for_status()
        job_id = response.json()["id"]
        duplicate = client.post("/jobs", json=body, headers={"Idempotency-Key": key})
        duplicate.raise_for_status()
        assert duplicate.json()["id"] == job_id
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            response = client.get(f"/jobs/{job_id}")
            response.raise_for_status()
            job = response.json()
            if job["status"] == "SUCCEEDED":
                break
            if job["status"] in {"FAILED", "CANCELLED"}:
                raise RuntimeError(job)
            time.sleep(0.5)
        else:
            raise TimeoutError("Job did not finish within 120 seconds")
        response = client.get(f"/jobs/{job_id}/result")
        response.raise_for_status()
        actual = list(csv.reader(io.StringIO(response.content.decode("utf-8-sig"))))
        with Path("samples/expected.csv").open(encoding="utf-8", newline="") as stream:
            expected = list(csv.reader(stream))
        assert actual == expected, (actual, expected)
        print(f"PASS: upload → idempotent submit → background execute → download ({job_id})")
        print(response.content.decode("utf-8-sig"))


if __name__ == "__main__":
    main()
