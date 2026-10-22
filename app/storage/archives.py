"""Durable gzip objects; never expose or remove the source before metadata commits."""

import gzip
import hashlib
import os
from collections.abc import Iterable
from pathlib import Path

from app.core.errors import ProcessingError
from app.models.entities import uid
from app.storage.local import LocalStorage


def write_gzip(key: str, chunks: Iterable[bytes]) -> tuple[Path, str]:
    storage = LocalStorage()
    final = storage.path(key)
    final.parent.mkdir(parents=True, exist_ok=True)
    temporary = final.with_suffix(f".{uid()}.tmp")
    try:
        with temporary.open("xb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as zipped:
                for chunk in chunks:
                    zipped.write(chunk)
            raw.flush()
            os.fsync(raw.fileno())
        os.replace(temporary, final)
        storage.sync_directory(final.parent)
        return final, storage.digest(final)
    finally:
        temporary.unlink(missing_ok=True)


def input_path(record: dict, attempt_directory: Path, index: int) -> Path:
    storage = LocalStorage()
    if not record["archive_key"]:
        return storage.path(record["storage_key"])
    output = attempt_directory / f"input-{index}.csv"
    digest, size = hashlib.sha256(), 0
    try:
        with (
            gzip.open(storage.path(record["archive_key"]), "rb") as source,
            output.open("xb") as target,
        ):
            while chunk := source.read(1024 * 1024):
                size += len(chunk)
                if size > record["size_bytes"]:
                    raise ProcessingError("ARCHIVE_CORRUPT", "归档输入超出原始文件大小")
                digest.update(chunk)
                target.write(chunk)
        if size != record["size_bytes"] or digest.hexdigest() != record["sha256"]:
            raise ProcessingError("ARCHIVE_CORRUPT", "归档输入完整性校验失败")
        return output
    except (gzip.BadGzipFile, EOFError) as exc:
        output.unlink(missing_ok=True)
        raise ProcessingError("ARCHIVE_CORRUPT", "归档文件损坏") from exc
    except Exception:
        output.unlink(missing_ok=True)
        raise
