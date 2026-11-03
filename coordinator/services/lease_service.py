"""Lease use cases. Names and signatures are placeholders for team design."""

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from coordinator.models.job import JobStatus
from coordinator.models.lease import (
    JobRequirements,
    Lease,
    LeaseOfferItem,
    LeaseOutcome,
    LeaseStatus,
    ProviderLeaseOffersResponse,
    RejectRequest,
)
from coordinator.policies.retry import should_retry
from coordinator.store import Store


# Declared ProviderNotFound locally here to keep the graph of dependencies linear
class ProviderNotFound(Exception):
    """The requested Provider does not exist; routes translate this to HTTP 404."""

    def __init__(self, provider_id: str):
        super().__init__(f"provider_id {provider_id!r} not found")
        self.provider_id = provider_id


class LeaseNotFound(Exception):
    """The requested Lease does not exist; routes translate this to HTTP 404."""

    def __init__(self, lease_id: str):
        super().__init__(f"lease_id {lease_id!r} not found")
        self.lease_id = lease_id

class LeaseProviderMismatch(Exception):
    """The provider does not own this lease; routes translate this to HTTP 403 or 409."""

    def __init__(self, lease_id: str, lease_provider_id: str, request_provider_id: str):
        super().__init__(
            f"lease {lease_id!r} is owned by {lease_provider_id!r}, but requested by {request_provider_id!r}"
        )
        self.lease_id = lease_id
        self.lease_provider_id = lease_provider_id
        self.request_provider_id = request_provider_id

class InvalidLeaseState(Exception):
    """The lease is not in the correct state for this action; routes translate this to HTTP 409."""

    def __init__(self, lease_id: str, current_status: str, expected_status: str):
        super().__init__(
            f"lease {lease_id!r} is in state {current_status!r}, expected {expected_status!r}"
        )
        self.lease_id = lease_id
        self.current_status = current_status
        self.expected_status = expected_status


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


# TODO: Refactor this for better extensions
# (if we have more valid states, this should be easier to find)
# All valid states
# Offered: 1, A provider can reject lease 2, An offered lease can expire
#          3, when heartbeat lost/drain lease can revoke
# Active: 1, active lease can expire 2, active lease(when drain/stale) can revoke
#         3, when job success/fail can release
_CLOSING_TRANSITIONS = {
    LeaseStatus.OFFERED: {LeaseStatus.REJECTED, LeaseStatus.EXPIRED, LeaseStatus.REVOKED},
    LeaseStatus.ACTIVE: {LeaseStatus.EXPIRED, LeaseStatus.REVOKED, LeaseStatus.RELEASED},
}


def close_lease(
    store: Store,
    lease_id: str,
    status: LeaseStatus,
    reason: Optional[str],
    now: datetime,
    max_attempts: int,
    outcome: Optional[LeaseOutcome] = None,
) -> None:
    """
    End a live Lease and decide what its Job does next. ASSUME CALLER HOLDS store.lock.
    """
    lease = store.leases[lease_id]
    if status not in _CLOSING_TRANSITIONS.get(lease.status, set()):
        # state transitions must be valid
        raise ValueError(f"cannot close lease {lease_id!r}: {lease.status.value} -> {status.value}")
    if (status == LeaseStatus.RELEASED) != (outcome is not None):
        # outcome = what the provider reported (SUCCESS/FAILURE), only report has it.
        # 1, RELEASED (from report) must come with an outcome
        # 2, every other status (no report) must not have one
        raise ValueError(f"lease {lease_id!r}: outcome must be given exactly when RELEASED")

    was_accepted = lease.status == LeaseStatus.ACTIVE
    lease.status = status
    lease.reason = reason
    lease.outcome = outcome
    # This is not useless, can be recorded for analysis on production :)
    if status == LeaseStatus.RELEASED:
        lease.released_at = now

    # Lease is done. Now decide what its Job does next, 4 possible cases:
    # 1, a new lease already took over the job -> job untouched
    # 2, lease was only offered, never accepted -> job stays QUEUED, attempts unchanged
    # 3, accepted and provider reported SUCCESS -> job COMPLETED
    # 4, accepted but did not succeed (failure report/timeout/stale/drain) -> retry check
    job = store.jobs[lease.job_id]

    # Case 1: old lease should not change job related stuff.
    # Safety net only: a live lease is always its job's current lease.
    if job.current_lease_id != lease_id:
        return

    # Cases 2-4 all end the job's current lease, so clear it.
    # Scheduler only offers jobs whose current_lease_id is None.
    job.current_lease_id = None

    # Case 2: job was QUEUED the whole offer, attempt_count only goes up on accept.
    if not was_accepted:
        return

    # Case 3
    if outcome == LeaseOutcome.SUCCESS:
        job.status = JobStatus.COMPLETED
        return

    # Case 4: keep the reason so a FAILED job explains itself,
    # then QUEUED if attempts are left, otherwise FAILED.
    job.last_failure_reason = reason
    if should_retry(job, reason, max_attempts):
        job.status = JobStatus.QUEUED
    else:
        job.status = JobStatus.FAILED


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
                    tier=job.tier,
                )

                offer_item = LeaseOfferItem(
                    lease=lease.model_copy(deep=True), requirements=requirements
                )

                offers.append(offer_item)

        return ProviderLeaseOffersResponse(offers=offers)

async def reject_lease(store: Store, lease_id: str, request: RejectRequest) -> Lease:
    async with store.lock:
        if lease_id not in store.leases:
            raise LeaseNotFound(lease_id)
        
        lease = store.leases[lease_id]

        if lease.provider_id != request.provider_id:
            raise LeaseProviderMismatch(lease_id, lease.provider_id, request.provider_id)

        if lease.status != LeaseStatus.OFFERED:
            raise InvalidLeaseState(lease_id, lease.status, LeaseStatus.OFFERED)

        job = store.jobs[lease.job_id]
        job.rejected_provider_ids.add(lease.provider_id)

        close_lease(store, lease_id, LeaseStatus.REJECTED, request.reason, datetime.now(timezone.utc), 0)

        return lease.model_copy(deep=True)
