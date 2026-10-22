from app.workers.celery_app import celery_app
from app.workers.runner import run_job


@celery_app.task(name="dataflow.run_job")
def execute(job_id: str):
    run_job(job_id)
