"""Stop real PostgreSQL exactly after a result is written, then prove lease recovery."""

import argparse
import hashlib
import json
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import httpx

from scripts.celery_benchmark import compose, verification_env, wait_ready


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="dataflow-verification")
    parser.add_argument("--port", type=int, default=18000)
    parser.add_argument("--output", type=Path, default=Path("docs/postgres-fault.json"))
    args = parser.parse_args()
    env, token = verification_env(args.port), str(uuid4())
    report = {
        "project": args.project,
        "fault": "real PostgreSQL stop after fsynced result",
        "sql_exception_mocking": False,
    }
    process = None
    log_path = Path("data/postgres-fault-worker.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with (
        httpx.Client(
            base_url=f"http://127.0.0.1:{args.port}/api/v1", trust_env=False, timeout=15
        ) as client,
        log_path.open("w+") as log,
    ):
        wait_ready(client)
        for state in ("QUEUED", "RUNNING", "RETRY_WAIT"):
            assert not client.get(f"/jobs?status={state}").json()["items"], "Test project is busy"
        with Path("samples/january.csv").open("rb") as stream:
            response = client.post("/files", files={"file": ("january.csv", stream)})
        response.raise_for_status()
        file_id = response.json()["file_id"]
        body = {"input_file_ids": [file_id], "steps": [{"type": "merge"}]}
        baseline = client.post("/jobs", json=body).json()["id"]
        for _ in range(300):
            if client.get(f"/jobs/{baseline}").json()["status"] == "SUCCEEDED":
                break
            time.sleep(0.1)
        expected = client.get(f"/jobs/{baseline}/result")
        expected.raise_for_status()
        compose(args.project, env, "stop", "worker", "dispatcher")
        try:
            response = client.post("/jobs", json=body)
            response.raise_for_status()
            job_id = response.json()["id"]
            command = [
                "docker",
                "compose",
                "-p",
                args.project,
                "exec",
                "-T",
                "-e",
                "DF_LEASE_SECONDS=6",
                "api",
                "python",
                "-",
                job_id,
                token,
            ]
            process = subprocess.Popen(
                command,
                env=env,
                stdin=subprocess.PIPE,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            process.stdin.write(Path("scripts/db_fault_worker.py").read_text())
            process.stdin.close()
            for _ in range(100):
                try:
                    compose(
                        args.project,
                        env,
                        "exec",
                        "-T",
                        "api",
                        "test",
                        "-f",
                        f"/data/faults/{token}/file-written",
                        timeout=10,
                    )
                    break
                except subprocess.CalledProcessError:
                    if process.poll() is not None:
                        raise RuntimeError(f"Fault worker exited early; see {log_path}") from None
                    time.sleep(0.1)
            else:
                raise TimeoutError("Worker never reached the commit barrier")
            report["file_written_before_outage"] = True
            compose(args.project, env, "stop", "-t", "1", "postgres")
            report["live_during_outage"] = client.get("/health/live").status_code
            report["ready_during_outage"] = client.get("/health/ready").status_code
            report["job_read_during_outage"] = client.get(f"/jobs/{job_id}").status_code
            assert report["live_during_outage"] == 200
            assert report["ready_during_outage"] == 503
            assert report["job_read_during_outage"] == 500
            compose(
                args.project, env, "exec", "-T", "api", "touch", f"/data/faults/{token}/release"
            )
            report["fault_worker_exit_code"] = process.wait(timeout=30)
            assert report["fault_worker_exit_code"] != 0
            print(
                "PASS: real PostgreSQL unavailable; commit failed, API returned 503/500", flush=True
            )
            time.sleep(7)  # expire the test attempt's six-second lease
            compose(args.project, env, "start", "postgres")
            wait_ready(client)
            before_recovery = client.get(f"/jobs/{job_id}").json()
            report["status_before_recovery"] = before_recovery["status"]
            report["result_before_recovery"] = client.get(f"/jobs/{job_id}/result").status_code
            assert before_recovery["status"] == "RUNNING"
            assert before_recovery["result_file_id"] is None
            assert report["result_before_recovery"] == 409
        finally:
            # Restore services even on an assertion failure; do not touch the normal project.
            compose(args.project, env, "start", "postgres", "dispatcher", "worker")
            if process and process.poll() is None:
                compose(
                    args.project, env, "exec", "-T", "api", "touch", f"/data/faults/{token}/release"
                )
                process.wait(timeout=30)
        for _ in range(600):
            response = client.get(f"/jobs/{job_id}")
            response.raise_for_status()
            recovered = response.json()
            if recovered["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                break
            time.sleep(0.2)
        assert recovered["status"] == "SUCCEEDED", recovered
        assert recovered["attempt_count"] == 2, recovered
        result = client.get(f"/jobs/{job_id}/result")
        result.raise_for_status()
        assert result.content == expected.content
        report.update(
            final_status=recovered["status"],
            attempts=recovered["attempt_count"],
            result_sha256=hashlib.sha256(result.content).hexdigest(),
            result_matches_baseline=True,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(f"PASS: no false success, expired attempt recovered, result identical; {args.output}")


if __name__ == "__main__":
    main()
