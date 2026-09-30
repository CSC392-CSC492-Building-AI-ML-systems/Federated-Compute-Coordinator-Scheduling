"""Job use cases. Names and signatures are placeholders for team design."""

from datetime import datetime
from coordinator.models.job import Job, JobSubmitRequest, JobStatus
from coordinator.store import Store
import uuid


class JobNotFound(Exception):
    """
    The requested job_id is not registered
    """
    pass


async def submit_job(store: Store, request: JobSubmitRequest, received_at: datetime) -> Job:
    """
    Create a queued job and return a copy of the stored record.
    received_at is the Coordinator's receive time, passed in by the caller.
    """

    # Create the new Job
    new_job = Job(
        job_id=str(uuid.uuid4()),
        status=JobStatus.QUEUED,
        required_vram_mb=request.required_vram_mb,
        gpu_models=list(request.gpu_models),
        required_runtime=request.required_runtime,
        tier=request.tier,
        created_at=received_at,
    )

    # I do not check for id conflicts here, however we should consider it perhaps.
    async with store.lock:
        store.jobs[new_job.job_id] = new_job
        return new_job.model_copy(deep=True)


        
async def get_job(store: Store, job_id: str) -> Job:
    """
    Fetches the Job with job_id inside the store, if it exists
    """
    async with store.lock:
        if job_id in store.jobs:
           return store.jobs[job_id].model_copy(deep=True)
        else:
            raise JobNotFound(f"Job '{job_id}' not found")








    
