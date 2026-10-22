import hashlib
import os
from pathlib import Path
from typing import BinaryIO

from app.core.config import get_settings
from app.core.errors import AppError
from app.models.entities import uid


class LocalStorage:
    def __init__(self):
        self.root = get_settings().storage_root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, key: str) -> Path:
        path = (self.root / key).resolve()
        if not path.is_relative_to(self.root) or path == self.root:
            raise ValueError("Invalid storage key")
        return path

    def upload(self, source: BinaryIO) -> tuple[str, int, str]:
        key = f"uploads/{uid()}.csv"
        final = self.path(key)
        final.parent.mkdir(parents=True, exist_ok=True)
        temporary = final.with_suffix(".tmp")
        size, digest = 0, hashlib.sha256()
        try:
            with temporary.open("xb") as target:
                while chunk := source.read(1024 * 1024):
                    size += len(chunk)
                    if size > get_settings().max_upload_bytes:
                        raise AppError("FILE_TOO_LARGE", "文件超过上传大小限制", 413)
                    target.write(chunk)
                    digest.update(chunk)
                if size == 0:
                    raise AppError("EMPTY_FILE", "不能上传空文件", 422)
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary, final)
            self.sync_directory(final.parent)
            return key, size, digest.hexdigest()
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def sync_directory(path: Path):
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def result_paths(self, job_id: str, attempt_id: str) -> tuple[str, Path, Path]:
        key = f"jobs/{job_id}/{attempt_id}/result.csv"
        final = self.path(key)
        final.parent.mkdir(parents=True, exist_ok=True)
        return key, final.with_suffix(".tmp"), final

    @staticmethod
    def digest(path: Path) -> str:
        with path.open("rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()
