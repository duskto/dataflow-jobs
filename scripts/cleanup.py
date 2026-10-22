"""Delete only aged unreferenced objects, excluding all active attempt directories."""

import argparse
import time

from sqlalchemy import and_, or_, select

from app.core.config import get_settings
from app.core.db import session_factory
from app.models.entities import EventArchive, File, Job
from app.storage.local import LocalStorage


def cleanup(apply: bool = False) -> list[str]:
    storage = LocalStorage()
    cutoff = time.time() - get_settings().orphan_grace_seconds
    # Protect RUNNING jobs even if they are processing longer than the grace period.
    with session_factory()() as session:
        protected = set(session.scalars(select(Job.id).where(Job.status == "RUNNING")))
    candidates = []
    for directory in (storage.root / "uploads", storage.root / "jobs", storage.root / "archives"):
        if not directory.exists():
            continue
        for path in directory.rglob("*"):
            if not path.is_file() or path.stat().st_mtime >= cutoff:
                continue
            key = path.relative_to(storage.root).as_posix()
            parts = key.split("/")
            if parts[0] == "jobs" and parts[1] in protected:
                continue
            # Recheck references and job status just before deleting. Immutable keys
            # are never reused by new attempts; the grace period protects upload commits.
            with session_factory()() as session:
                if session.scalar(
                    select(File.id).where(
                        or_(
                            File.archive_key == key,
                            and_(File.archive_key.is_(None), File.storage_key == key),
                        )
                    )
                ):
                    continue
                if session.scalar(
                    select(EventArchive.job_id).where(EventArchive.storage_key == key)
                ):
                    continue
                if parts[0] == "jobs" and session.scalar(
                    select(Job.id).where(Job.id == parts[1], Job.status == "RUNNING")
                ):
                    continue
            candidates.append(key)
            if apply:
                path.unlink(missing_ok=True)
    return candidates


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Actually delete (default: preview)")
    args = parser.parse_args()
    for key in cleanup(args.apply):
        print(key)
