"""Jobs HTTP routes: validate input, call services, return responses."""
from datetime import datetime
from fastapi import APIRouter, Depends

from coordinator.api.tools.dependencies import get_received_at, get_store
from coordinator.api.tools.errors import ApiError
from coordinator.models.job import JobSubmitRequest, Job
from coordinator.services import job_service
from coordinator.store import Store

router = APIRouter()

# TODO: Define endpoints and request/response models.
# Keep state changes in services, not in route handlers.

# 201 Created
@router.post("/jobs", status_code=201, response_model=Job)
async def submit_job(
    payload: JobSubmitRequest, 
    store: Store = Depends(get_store), 
    received_at: datetime = Depends(get_received_at)):
    """
    Endpoint to submit a new job to the coordinator
    """
    job = await job_service.submit_job(store, payload, received_at)
    return job

# 200 Created
@router.get("/jobs/{job_id}", status_code=200, response_model=Job)
async def get_job(
    job_id: str,
    store: Store = Depends(get_store)):
    """
    Endpoint to obtain a job with the requested job_id, if it exists.
    """
    try:
        return await job_service.get_job(store, job_id)
    except job_service.JobNotFound as exc:
        raise ApiError(404, "JOB_NOT_FOUND", str(exc)) from exc



