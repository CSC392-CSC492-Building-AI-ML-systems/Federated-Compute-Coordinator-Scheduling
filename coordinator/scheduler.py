"""The Job Scheduler. Orchestrates the creation of OFFERED leases
between QUEUED jobs and ACTIVE providers."""

import asyncio
import logging
from datetime import datetime
from typing import Callable

from coordinator.models.job import JobStatus
from coordinator.policies.matching import can_run
from coordinator.services.lease_service import create_lease
from coordinator.settings import Settings
from coordinator.store import Store

logger = logging.getLogger(__name__)


async def run_matching(now: datetime, store: Store) -> None:
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


async def scheduler_loop(store: Store, clock: Callable[[], datetime], settings: Settings) -> None:
    """
    Infinite background loop driving the scheduling ticks.
    """
    while True:
        try:
            await run_matching(clock(), store)
        except asyncio.CancelledError:
            # Re-raise to allow clean task cancellation on FastAPI shutdown
            raise
        except Exception:
            # Trap unexpected errors so the background worker doesn't die
            logger.exception("Scheduler tick failed")

        await asyncio.sleep(settings.scheduler_interval_seconds)
