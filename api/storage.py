import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def get_engine():
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")

    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD", "")

    if password:
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    else:
        url = f"postgresql+psycopg2://{user}@{host}:{port}/{db}"

    return create_engine(url)


def init_db():
    engine = get_engine()
    sql = """
    CREATE TABLE IF NOT EXISTS etl_job_history (
        id VARCHAR(36) PRIMARY KEY,
        job_type VARCHAR(32) NOT NULL,
        status VARCHAR(32) NOT NULL,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        duration_sec NUMERIC(10,2),
        rows_read INTEGER,
        rows_written INTEGER,
        error_text TEXT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def create_job(job_id: str, job_type: str, status: str = "queued"):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO etl_job_history (id, job_type, status)
                VALUES (:id, :job_type, :status)
            """),
            {"id": job_id, "job_type": job_type, "status": status},
        )


def start_job(job_id: str):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE etl_job_history
                SET status = 'running', started_at = CURRENT_TIMESTAMP
                WHERE id = :id
            """),
            {"id": job_id},
        )


def finish_job_success(job_id: str, metrics: dict):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE etl_job_history
                SET status = 'success',
                    finished_at = CURRENT_TIMESTAMP,
                    duration_sec = :duration_sec,
                    rows_read = :rows_read,
                    rows_written = :rows_written
                WHERE id = :id
            """),
            {
                "id": job_id,
                "duration_sec": metrics.get("duration_sec"),
                "rows_read": metrics.get("rows_read"),
                "rows_written": metrics.get("rows_written"),
            },
        )


def finish_job_failed(job_id: str, error_text: str):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE etl_job_history
                SET status = 'failed',
                    finished_at = CURRENT_TIMESTAMP,
                    error_text = :error_text
                WHERE id = :id
            """),
            {"id": job_id, "error_text": error_text},
        )


def get_job(job_id: str):
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT id, job_type, status, started_at, finished_at,
                       duration_sec, rows_read, rows_written, error_text
                FROM etl_job_history
                WHERE id = :id
            """),
            {"id": job_id},
        ).mappings().first()
        return dict(row) if row else None


def get_history(limit: int = 20):
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, job_type, status, started_at, finished_at,
                       duration_sec, rows_read, rows_written, error_text
                FROM etl_job_history
                ORDER BY COALESCE(started_at, finished_at) DESC NULLS LAST
                LIMIT :limit
            """),
            {"limit": limit},
        ).mappings().all()
        return [dict(row) for row in rows]