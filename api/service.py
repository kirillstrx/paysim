from api.storage import start_job, finish_job_success, finish_job_failed
from etl.full_snapshot import run_full_snapshot
from etl.incremental_load import run_incremental_load


def execute_full_job(job_id: str):
    try:
        start_job(job_id)
        metrics = run_full_snapshot()
        finish_job_success(job_id, metrics)
    except Exception as e:
        finish_job_failed(job_id, str(e))


def execute_incremental_job(job_id: str):
    try:
        start_job(job_id)
        metrics = run_incremental_load()
        finish_job_success(job_id, metrics)
    except Exception as e:
        finish_job_failed(job_id, str(e))