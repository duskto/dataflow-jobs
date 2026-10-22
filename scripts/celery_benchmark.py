"""Sustained HTTP + PostgreSQL + Redis + Celery test in a dedicated Compose project."""

import argparse
import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

import httpx

STEPS = [
    {"type": "merge"},
    {"type": "deduplicate", "keys": ["order_id"]},
    {"type": "aggregate", "group_by": ["region"], "sum": ["amount"]},
]


def verification_env(port=18000):
    return dict(
        os.environ,
        DF_API_PORT=str(port),
        DF_WORKER_CONCURRENCY="1",
        DF_PROMETHEUS_PORT="19090",
        DF_ALERTMANAGER_PORT="19093",
        DF_MAX_ROWS="10000000",
        DF_MAX_UPLOAD_BYTES="134217728",
        DF_API_TOKEN="",
    )


def compose(project, env, *args, timeout=120):
    return subprocess.run(
        ["docker", "compose", "-p", project, *args],
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    ).stdout


def wait_ready(client, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if client.get("/health/ready").status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(1)
    raise TimeoutError("Verification API did not become ready")


def percentile(values, quantile):
    return sorted(values)[max(0, math.ceil(len(values) * quantile) - 1)] if values else None


def memory_bytes(value):
    number = value.split(" / ")[0]
    units = {
        "GiB": 1024**3,
        "MiB": 1024**2,
        "KiB": 1024,
        "GB": 10**9,
        "MB": 10**6,
        "kB": 1000,
        "B": 1,
    }
    for unit, multiplier in units.items():
        if number.endswith(unit):
            return int(float(number[: -len(unit)]) * multiplier)
    raise ValueError(number)


class MemorySampler:
    def __init__(self, container_ids):
        self.container_ids = container_ids
        self.peak = self.samples = 0
        self.errors = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        while not self.stop.is_set():
            try:
                output = subprocess.run(
                    [
                        "docker",
                        "stats",
                        "--no-stream",
                        "--format",
                        "{{.MemUsage}}",
                        *self.container_ids,
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=8,
                ).stdout
                values = [memory_bytes(line) for line in output.strip().splitlines()]
                self.peak = max(self.peak, sum(values))
                self.samples += 1
            except Exception as exc:
                self.errors.append(type(exc).__name__)
            self.stop.wait(1)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=10)


def run_case(client, file_id, workers, duration, project, env, expected_sha, input_bytes):
    active = client.get("/jobs?limit=100").json()["items"]
    if any(j["status"] in {"QUEUED", "RUNNING", "RETRY_WAIT"} for j in active):
        raise RuntimeError("Refusing to rescale workers with unfinished jobs in the test project")
    compose(project, env, "up", "-d", "--no-build", "--scale", f"worker={workers}", "worker")
    ids = compose(project, env, "ps", "-q", "worker").split()
    pending, completed, waits, execution, latency = set(), [], [], [], []
    start, last_report = time.monotonic(), 0.0
    stop_submitting = start + duration
    deadline = stop_submitting + 180
    with MemorySampler(ids) as memory:
        while time.monotonic() < deadline:
            while time.monotonic() < stop_submitting and len(pending) < workers * 2:
                response = client.post("/jobs", json={"input_file_ids": [file_id], "steps": STEPS})
                response.raise_for_status()
                pending.add(response.json()["id"])
            for job_id in list(pending):
                response = client.get(f"/jobs/{job_id}")
                response.raise_for_status()
                job = response.json()
                if job["status"] not in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                    continue
                pending.remove(job_id)
                ok = job["status"] == "SUCCEEDED"
                if ok:
                    result = client.get(f"/jobs/{job_id}/result")
                    result.raise_for_status()
                    actual = hashlib.sha256(result.content).hexdigest()
                    if expected_sha is None:
                        expected_sha = actual
                    ok = actual == expected_sha
                completed.append(ok)
                for attempt in job["attempts"]:
                    if attempt["finished_at"]:
                        execution.append(
                            (
                                datetime.fromisoformat(attempt["finished_at"])
                                - datetime.fromisoformat(attempt["started_at"])
                            ).total_seconds()
                        )
                    if attempt.get("queue_seconds") is not None:
                        waits.append(attempt["queue_seconds"])
                latency.append(
                    (
                        datetime.fromisoformat(job["finished_at"])
                        - datetime.fromisoformat(job["created_at"])
                    ).total_seconds()
                )
            elapsed = time.monotonic() - start
            if elapsed - last_report >= 15:
                print(
                    f"workers={workers} input={input_bytes} elapsed={elapsed:.0f}s "
                    f"completed={len(completed)} pending={len(pending)}",
                    flush=True,
                )
                last_report = elapsed
            if time.monotonic() >= stop_submitting and not pending:
                break
            time.sleep(0.1)
        if pending:
            for job_id in pending:
                client.post(f"/jobs/{job_id}/cancel")
            raise TimeoutError(f"{len(pending)} jobs exceeded drain deadline")
        elapsed = time.monotonic() - start
    failures = completed.count(False)
    return {
        "workers": workers,
        "worker_processes_per_container": 1,
        "submit_duration_seconds": duration,
        "elapsed_with_drain_seconds": round(elapsed, 3),
        "input_bytes_per_job": input_bytes,
        "completed": len(completed),
        "failures": failures,
        "error_rate": failures / max(1, len(completed)),
        "throughput_jobs_per_second": round(sum(completed) / elapsed, 4),
        "mean_queue_seconds": round(statistics.mean(waits), 4) if waits else None,
        "p95_queue_seconds": percentile(waits, 0.95),
        "mean_execution_seconds": round(statistics.mean(execution), 4),
        "p95_execution_seconds": percentile(execution, 0.95),
        "p95_end_to_end_seconds": percentile(latency, 0.95),
        "peak_worker_container_memory_bytes_sampled": memory.peak,
        "memory_samples": memory.samples,
        "memory_sample_errors": memory.errors,
        "result_sha256": expected_sha,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="dataflow-verification")
    parser.add_argument("--port", type=int, default=18000)
    parser.add_argument(
        "--inputs",
        type=Path,
        nargs="+",
        default=[Path("data/benchmark.csv"), Path("data/benchmark-100mb.csv")],
    )
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--soak-seconds", type=int, default=180)
    parser.add_argument("--output", type=Path, default=Path("docs/benchmark-celery.json"))
    args = parser.parse_args()
    if args.duration < 5 or any(n < 1 or n > 8 for n in args.workers):
        parser.error("duration must be >=5 and workers must be 1..8")
    env = verification_env(args.port)
    compose(
        args.project,
        env,
        "up",
        "-d",
        "--no-build",
        "--scale",
        "worker=1",
        timeout=240,
    )
    report = {
        "started_at": datetime.now().astimezone().isoformat(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "mode": "HTTP + PostgreSQL + Redis + Celery",
        "memory_method": (
            "docker stats loop with 1s pause plus command time; sum of worker containers only"
        ),
        "load": "closed loop; at most 2 unfinished jobs per worker; includes drain time",
        "results": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def save():
        args.output.write_text(json.dumps(report, indent=2) + "\n")

    with httpx.Client(
        base_url=f"http://127.0.0.1:{args.port}/api/v1", trust_env=False, timeout=60
    ) as client:
        wait_ready(client)
        uploaded = []
        for source in args.inputs:
            with source.open("rb") as stream:
                response = client.post("/files", files={"file": (source.name, stream)})
            response.raise_for_status()
            file_id = response.json()["file_id"]
            expected = None
            for workers in args.workers:
                result = run_case(
                    client,
                    file_id,
                    workers,
                    args.duration,
                    args.project,
                    env,
                    expected,
                    source.stat().st_size,
                )
                expected = result["result_sha256"]
                report["results"].append({"kind": "matrix", **result})
                save()
                print(json.dumps(result), flush=True)
            uploaded.append((file_id, source.stat().st_size, expected))
        if args.soak_seconds:
            file_id, size, expected = uploaded[0]
            result = run_case(
                client,
                file_id,
                max(args.workers),
                args.soak_seconds,
                args.project,
                env,
                expected,
                size,
            )
            report["results"].append({"kind": "soak", **result})
            save()
            print(json.dumps(result), flush=True)
    assert all(r["failures"] == 0 and r["memory_samples"] > 0 for r in report["results"]), report
    print(f"PASS: wrote {args.output}; isolated project {args.project} remains running", flush=True)


if __name__ == "__main__":
    main()
