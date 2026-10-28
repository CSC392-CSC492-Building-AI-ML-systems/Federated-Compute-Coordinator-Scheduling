"""Lease use cases. Names and signatures are placeholders for team design."""

import uuid
from datetime import datetime, timedelta

from coordinator.models.lease import Lease, LeaseStatus, JobRequirements, LeaseOfferItem, ProviderLeaseOffersResponse
from coordinator.store import Store
from coordinator.services.provider_service import ProviderNotFound


async def accept_lease(lease_id):
    """Accept an offered lease."""
    raise NotImplementedError("Team implementation pending")


async def report_lease(lease_id, report):
    """Handle a result and related state changes."""
    raise NotImplementedError("Team implementation pending")




def create_lease(
    store: Store, job_id: str, provider_id: str, now: datetime, timeout: float
) -> Lease:
    """
    Create an offered lease and return a copy of the stored record.

    """
    new_lease = Lease(
        lease_id=str(uuid.uuid4()),
        job_id=job_id,
        provider_id=provider_id,
        status=LeaseStatus.OFFERED,
        offer_deadline=now + timedelta(seconds=timeout),
        created_at=now,
    )

    store.leases[new_lease.lease_id] = new_lease
    return new_lease.model_copy(deep=True)

async def get_provider_lease_offers(store: Store, provider_id: str) -> ProviderLeaseOffersResponse:
    async with store.lock:

        if provider_id not in store.providers:
            raise ProviderNotFound(provider_id)

        offers = []
        for lease in store.leases.values():
            if lease.provider_id == provider_id and lease.status == LeaseStatus.OFFERED:
                job = store.jobs[lease.job_id]
                
                requirements = JobRequirements(
                    required_vram_mb=job.required_vram_mb,
                    gpu_models=list(job.gpu_models), 
                    required_runtime=job.required_runtime,
                    tier=job.tier
                )
                
                offer_item = LeaseOfferItem(
                    lease=lease.model_copy(deep=True),
                    requirements=requirements
                )
                
                offers.append(offer_item)
                
        return ProviderLeaseOffersResponse(offers=offers)
