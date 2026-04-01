import uuid
from fastapi import FastAPI, BackgroundTasks, HTTPException, Path, Query

from api.schemas import JobCreateResponse, JobStatusResponse
from api.storage import init_db, create_job, get_job, get_history
from api.service import execute_full_job, execute_incremental_job

app = FastAPI(
    title="PaySim ETL Service",
    description="REST API for full and incremental ETL jobs",
    version="1.0.0",
)


@app.on_event("startup")
def on_startup():
    init_db()


@app.post("/etl/full", response_model=JobCreateResponse, tags=["ETL"])
def run_full(background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    create_job(job_id=job_id, job_type="full", status="queued")
    background_tasks.add_task(execute_full_job, job_id)
    return JobCreateResponse(
        id=job_id,
        status="queued",
        message="Full snapshot job started in background",
    )


@app.post("/etl/incremental", response_model=JobCreateResponse, tags=["ETL"])
def run_incremental(background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    create_job(job_id=job_id, job_type="incremental", status="queued")
    background_tasks.add_task(execute_incremental_job, job_id)
    return JobCreateResponse(
        id=job_id,
        status="queued",
        message="Incremental job started in background",
    )


@app.get("/etl/status/{job_id}", response_model=JobStatusResponse, tags=["ETL"])
def etl_status(
    job_id: str = Path(..., min_length=36, max_length=36, description="UUID of ETL job"),
):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobStatusResponse(**job)


@app.get("/etl/history", tags=["ETL"])
def etl_history(
    limit: int = Query(20, ge=1, le=100, description="How many last jobs to return"),
):
    return get_history(limit=limit)