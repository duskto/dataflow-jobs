"""Isolated local-process benchmark; writes measured data, never estimated claims."""

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# Running as a module keeps app imports available with uv.
from alembic import command
from alembic.config import Config
from sqlalchemy import select

from app.core.config import get_settings
from app.core.db import get_engine, session_factory
from app.models.entities import File, Job, JobAttempt
from app.schemas.jobs import CreateJob
from app.services.jobs import create_job
from app.storage.local import LocalStorage


def memory_kib(pids: list[int]) -> int:
    total = 0
    for pid in pids:
        try:
            lines = Path(f"/proc/{pid}/status").read_text().splitlines()
            total += next(int(line.split()[1]) for line in lines if line.startswith("VmRSS:"))
        except (FileNotFoundError, StopIteration):
            pass
    return total


def measure(source: Path, workers: int, count: int, timeout: int) -> dict:
    with tempfile.TemporaryDirectory(prefix="dataflow-bench-") as folder:
        os.environ["DF_DATABASE_URL"] = f"sqlite:///{folder}/bench.db"
        os.environ["DF_STORAGE_ROOT"] = f"{folder}/storage"
        os.environ["DF_MAX_ROWS"] = "10000000"
        os.environ["DF_MAX_UPLOAD_BYTES"] = str(1024 * 1024 * 1024)
        get_settings.cache_clear()
        get_engine.cache_clear()
        command.upgrade(Config("alembic.ini"), "head")
        with source.open("rb") as stream:
            key, size, digest = LocalStorage().upload(stream)
        with session_factory().begin() as session:
            file = File(
                owner_id=get_settings().owner_id,
                original_name=source.name,
                storage_key=key,
                size_bytes=size,
                sha256=digest,
            )
            session.add(file)
            session.flush()
            file_id = file.id
        body = CreateJob(
            input_file_ids=[file_id],
            steps=[
                {"type": "merge"},
                {"type": "deduplicate", "keys": ["order_id"]},
                {"type": "aggregate", "group_by": ["region"], "sum": ["amount"]},
            ],
        )
        jobs = [create_job(body, get_settings().owner_id).id for _ in range(count)]
        children, peak = [], 0
        start = time.monotonic()
        try:
            for _ in range(workers):
                children.append(
                    subprocess.Popen(
                        [sys.executable, "-m", "app.workers.local"], stdout=subprocess.DEVNULL
                    )
                )
            while True:
                peak = max(peak, memory_kib([p.pid for p in children]))
                with session_factory()() as session:
                    statuses = session.scalars(select(Job.status).where(Job.id.in_(jobs))).all()
                if all(status in {"SUCCEEDED", "FAILED", "CANCELLED"} for status in statuses):
                    break
                if time.monotonic() - start > timeout:
                    raise TimeoutError("Benchmark timeout")
                time.sleep(0.05)
            elapsed = time.monotonic() - start
            with session_factory()() as session:
                pairs = session.execute(select(Job, JobAttempt).join(JobAttempt)).all()
                queue = [(a.started_at - j.created_at).total_seconds() for j, a in pairs]
                execution = [(a.finished_at - a.started_at).total_seconds() for _, a in pairs]
            succeeded = statuses.count("SUCCEEDED")
            return {
                "workers": workers,
                "jobs": count,
                "input_bytes_per_job": size,
                "elapsed_seconds": round(elapsed, 3),
                "throughput_jobs_per_second": round(succeeded / elapsed, 4),
                "mean_queue_seconds": round(statistics.mean(queue), 3),
                "mean_execution_seconds": round(statistics.mean(execution), 3),
                "peak_worker_rss_kib_sampled": peak,
                "error_rate": (count - succeeded) / count,
            }
        finally:
            for child in children:
                child.terminate()
            for child in children:
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            get_engine().dispose()
            get_engine.cache_clear()
            get_settings.cache_clear()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--jobs", type=int, default=8)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--output", type=Path, default=Path("data/benchmark.json"))
    args = parser.parse_args()
    report = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "mode": "local SQLite, warm input, process startup included",
        "memory_measurement": (
            "Sum of worker RSS sampled every 50ms; shared pages count per process"
        ),
        "results": [],
    }
    for workers in args.workers:
        result = measure(args.input, workers, args.jobs, args.timeout)
        report["results"].append(result)
        print(json.dumps(result), flush=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
