"""Jobs HTTP routes: validate input, call services, return responses."""
from datetime import datetime
from fastapi import APIRouter, Depends

from coordinator.api.tools.dependencies import get_received_at, get_store
from coordinator.models.job import JobSubmitRequest, Job
from coordinator.services.job_service import submit_job as create_job
from coordinator.store import Store

router = APIRouter()

# TODO: Define endpoints and request/response models.
# Keep state changes in services, not in route handlers.

@router.post("/jobs", status_code=201, response_model=Job)
async def submit_job(
    payload: JobSubmitRequest, 
    store: Store = Depends(get_store), 
    received_at: datetime = Depends(get_received_at)):
    """
    Endpoint to submit a new job to the coordinator
    """
    job = await create_job(store, payload, received_at)
    return job



