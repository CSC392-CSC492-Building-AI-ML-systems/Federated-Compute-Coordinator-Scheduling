"""Lease use cases. Names and signatures are placeholders for team design."""

import uuid
from datetime import datetime, timedelta, timezone

from coordinator.models.lease import Lease, LeaseStatus
from coordinator.store import Store


async def accept_lease(lease_id):
    """Accept an offered lease."""
    raise NotImplementedError("Team implementation pending")


async def report_lease(lease_id, report):
    """Handle a result and related state changes."""
    raise NotImplementedError("Team implementation pending")


def create_lease(store: Store, job_id: str, provider_id: str) -> Lease:
    """
    Create an offered lease and return a copy of the stored record.

    """
    now = datetime.now(timezone.utc)
    new_lease = Lease(
        lease_id=str(uuid.uuid4()),
        job_id=job_id,
        provider_id=provider_id,
        status=LeaseStatus.OFFERED,
        offer_deadline=now
        + timedelta(
            hours=1
        ),  # Temporarily sets to 1 hour as we have not decided on the deadline duration.
        created_at=now,
    )

    store.leases[new_lease.lease_id] = new_lease
    return new_lease.model_copy(deep=True)
