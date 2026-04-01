from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class JobCreateResponse(BaseModel):
    id: str
    status: str
    message: str


class JobStatusResponse(BaseModel):
    id: str
    job_type: str
    status: str
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_sec: Optional[float] = None
    rows_read: Optional[int] = None
    rows_written: Optional[int] = None
    error_text: Optional[str] = None