"""Decide whether a failed attempt should be retried."""

from typing import Optional

from coordinator.models.job import Job


def should_retry(job: Job, reason: Optional[str], max_attempts: int) -> bool:
    # I think we said some reasons are retryable some are not
    return job.attempt_count < max_attempts
