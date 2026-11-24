"""Job use cases. Names and signatures are placeholders for team design."""

import uuid
from datetime import datetime, timedelta

from coordinator.models.job import Job, JobStatus, JobSubmitRequest
from coordinator.models.lease import InternalReason
from coordinator.store import Store


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


async def expire_queued_jobs(store: Store, now: datetime, queue_timeout_seconds: float) -> None:
    """
    Fail Jobs that waited in the queue too long. No Provider took them in time.
    """
    # 1, the Job is not QUEUED -> leave it
    # 2, the Job has an open offer -> leave it this time
    # 3, the Job has not waited long enough yet -> leave it
    # 4, the Job waited too long -> FAILED with QUEUE_TIMEOUT
    limit = timedelta(seconds=queue_timeout_seconds)
    async with store.lock:
        for job in store.jobs.values():
            # Case 1: it is already running or done.
            if job.status != JobStatus.QUEUED:
                continue

            # Case 2: a Provider is deciding right now and may still accept.
            # Once the offer ends, the next check looks at this Job again.
            if job.current_lease_id is not None:
                continue

            # Case 3: time counts from submission.
            # A Job that ran, failed and came back to the queue keeps that start time.
            # Reaching the limit counts as too long, like every other deadline.
            if now - job.created_at < limit:
                continue

            # Case 4: attempt_count stays the same. Only accept uses an attempt.
            job.status = JobStatus.FAILED
            job.last_failure_reason = InternalReason.QUEUE_TIMEOUT.value
