import logging
import os
import socket
import threading

from sqlalchemy.exc import SQLAlchemyError

from app.core.config import get_settings
from app.core.errors import Cancelled, LeaseLost, ProcessingError
from app.processors.pipeline import process
from app.services.jobs import claim, finish_failure, finish_success, heartbeat
from app.storage.archives import input_path
from app.storage.local import LocalStorage

logger = logging.getLogger(__name__)


class LeaseGuard:
    """A heartbeat thread covers blocking I/O as well as processor checkpoints."""

    def __init__(self, job_id: str, attempt_id: str):
        self.job_id, self.attempt_id = job_id, attempt_id
        self.phase, self.rows = "读取", 0
        self.error: Exception | None = None
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self):
        while not self.stop.wait(get_settings().lease_seconds / 3):
            try:
                heartbeat(self.job_id, self.attempt_id, self.phase, self.rows)
            except Exception as exc:
                self.error = exc
                return

    def progress(self, phase: str, rows: int):
        if self.error:
            raise self.error
        self.phase, self.rows = phase, rows

    def __enter__(self):
        heartbeat(self.job_id, self.attempt_id, self.phase, self.rows)
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=get_settings().lease_seconds)


def run_job(job_id: str):
    claimed = claim(job_id, f"{socket.gethostname()}:{os.getpid()}")
    if claimed is None:
        return
    attempt_id, payload = claimed
    temporary = None
    try:
        storage = LocalStorage()
        key, temporary, final = storage.result_paths(job_id, attempt_id)
        settings = get_settings()
        with LeaseGuard(job_id, attempt_id) as guard:
            paths = [
                input_path(record, temporary.parent, index)
                for index, record in enumerate(payload["inputs"])
            ]
            process(
                paths,
                temporary,
                payload["steps"],
                payload["options"],
                max_rows=settings.max_rows,
                max_unique_keys=settings.max_unique_keys,
                max_groups=settings.max_groups,
                max_columns=settings.max_columns,
                max_field_chars=settings.max_field_chars,
                progress=guard.progress,
            )
            guard.progress("提交结果", guard.rows)
            heartbeat(job_id, attempt_id, "提交结果", guard.rows)
            os.replace(temporary, final)
            storage.sync_directory(final.parent)
            digest = storage.digest(final)
            if not finish_success(job_id, attempt_id, key, final.stat().st_size, digest):
                # Cancellation can win after the final heartbeat.
                finish_failure(job_id, attempt_id, "CANCELLED", "取消请求已生效")
    except LeaseLost:
        logger.info("Lease lost for job=%s attempt=%s", job_id, attempt_id)
    except Cancelled:
        finish_failure(job_id, attempt_id, "CANCELLED", "取消请求已生效")
    except ProcessingError as exc:
        finish_failure(job_id, attempt_id, exc.code, str(exc))
    except FileNotFoundError:
        finish_failure(job_id, attempt_id, "INPUT_MISSING", "输入文件丢失")
    except OSError:
        logger.exception("Storage failure job=%s", job_id)
        finish_failure(job_id, attempt_id, "STORAGE_UNAVAILABLE", "暂时无法访问存储", True)
    except SQLAlchemyError:
        # If the DB itself is unavailable, this may also fail. The lease then expires
        # and the recovery process decides the next attempt; no Celery retry counter.
        logger.exception("Execution failure job=%s", job_id)
        finish_failure(job_id, attempt_id, "EXECUTION_ERROR", "执行服务异常", True)
    except Exception:
        logger.exception("Unexpected execution failure job=%s", job_id)
        finish_failure(job_id, attempt_id, "INTERNAL_ERROR", "执行出现不可重试的内部错误")
    finally:
        if temporary:
            temporary.unlink(missing_ok=True)
            for materialized in temporary.parent.glob("input-*.csv"):
                materialized.unlink(missing_ok=True)
