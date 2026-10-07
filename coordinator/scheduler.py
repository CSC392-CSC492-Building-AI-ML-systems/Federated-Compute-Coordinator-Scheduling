"""The Job Scheduler. Orchestrates the creation of OFFERED leases
between QUEUED jobs and ACTIVE providers."""
import asyncio
import logging
from datetime import datetime

from fastapi import Depends

from coordinator.api.tools.dependencies import get_store
from coordinator.models.job import JobStatus
from coordinator.policies.matching import can_run
from coordinator.services.lease_service import create_lease
from coordinator.store import Store


async def run_matching(now: datetime, store: Store = Depends(get_store) ):
    """
    Iterates through all QUEUED jobs and attempts to find
    a provider for them.
    """
    assigned_providers = set()
    async with store.lock:
        for job in store.jobs.values():
            if job.status != JobStatus.QUEUED or job.current_lease_id is not None:
                continue

            for provider in store.providers.values():
                if provider.provider_id not in assigned_providers and can_run(provider, job):
                    lease = create_lease(store, job.job_id, provider.provider_id, now)
                    job.current_lease_id = lease.lease_id
                    assigned_providers.add(provider.provider_id)
                    break
