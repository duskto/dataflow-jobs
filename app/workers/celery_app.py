from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown

from app.core.config import get_settings
from app.core.db import get_engine
from app.observability.heartbeat import ServiceHeartbeat

celery_app = Celery("dataflow", broker=get_settings().redis_url, include=["app.workers.tasks"])
celery_app.conf.update(
    task_ignore_result=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_serializer="json",
    accept_content=["json"],
    broker_connection_retry_on_startup=True,
    broker_transport_options={
        "visibility_timeout": 3600,
        "socket_timeout": 5,
        "socket_connect_timeout": 5,
    },
)


_heartbeat = None


@worker_process_init.connect
def reset_pool(**_):
    global _heartbeat
    get_engine().dispose(close=False)
    _heartbeat = ServiceHeartbeat("worker").start()


@worker_process_shutdown.connect
def stop_heartbeat(**_):
    if _heartbeat:
        _heartbeat.close()
