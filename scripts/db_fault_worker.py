"""Test-only commit barrier. SQL/connection failures themselves are never mocked."""

import sys
import time
from pathlib import Path
from uuid import UUID

from app.workers import runner

job_id = str(UUID(sys.argv[1]))
token = str(UUID(sys.argv[2]))
barrier = Path("/data/faults") / token
barrier.mkdir(parents=True, exist_ok=True)
real_commit = runner.finish_success


def wait_at_commit(*args, **kwargs):
    # runner has fsynced the complete immutable result before calling this function.
    (barrier / "file-written").write_text("ready")
    deadline = time.monotonic() + 120
    while not (barrier / "release").exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("Commit barrier timed out")
        time.sleep(0.05)
    return real_commit(*args, **kwargs)


runner.finish_success = wait_at_commit
runner.run_job(job_id)
