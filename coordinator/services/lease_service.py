"""Lease use cases. Names and signatures are placeholders for team design."""

import uuid
from datetime import datetime, timedelta
from typing import Optional

from coordinator.models.job import JobStatus
from coordinator.models.lease import (
    AcceptResponse,
    InternalReason,
    JobRequirements,
    Lease,
    LeaseActionRequest,
    LeaseOfferItem,
    LeaseOutcome,
    LeaseStatus,
    ProviderLeaseOffersResponse,
    RejectRequest,
    RejectResponse,
    StartedResponse,
)
from coordinator.models.provider import ProviderStatus
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
            f"lease {lease_id!r} is owned by {lease_provider_id!r}, "
            f"but requested by {request_provider_id!r}"
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


class OfferExpired(Exception):
    """The offer deadline has been reached; routes translate this to HTTP 409."""

    def __init__(self, lease_id: str, offer_deadline: datetime):
        super().__init__(f"lease {lease_id!r} offer expired at {offer_deadline.isoformat()}")
        self.lease_id = lease_id
        self.offer_deadline = offer_deadline


class ProviderNotActive(Exception):
    """The Provider's status does not allow this action; routes translate this to HTTP 409."""

    def __init__(self, provider_id: str, status: str):
        super().__init__(f"provider_id {provider_id!r} is {status!r}, which does not allow this")
        self.provider_id = provider_id
        self.status = status


class LeaseNotCurrent(Exception):
    """The Job no longer waits for this Lease; routes translate this to HTTP 409."""

    def __init__(self, lease_id: str, job_id: str):
        super().__init__(f"lease {lease_id!r} is not the current lease of job {job_id!r}")
        self.lease_id = lease_id
        self.job_id = job_id


class LeaseExpired(Exception):
    """The lease's execution deadline has been reached; routes translate this to HTTP 409."""

    def __init__(self, lease_id: str, expires_at: datetime):
        super().__init__(f"lease {lease_id!r} expired at {expires_at.isoformat()}")
        self.lease_id = lease_id
        self.expires_at = expires_at


class InvalidJobState(Exception):
    """The Job is not in the state this action needs; routes translate this to HTTP 409."""

    def __init__(self, job_id: str, current_status: str, expected_status: str):
        super().__init__(
            f"job {job_id!r} is in state {current_status!r}, expected {expected_status!r}"
        )
        self.job_id = job_id
        self.current_status = current_status
        self.expected_status = expected_status


def _find_owned_lease(
    store: Store, lease_id: str, provider_id: str, expected_status: LeaseStatus
) -> Lease:
    """
    Find the lease, and check it belongs to this Provider and has the right status.
    Caller holds store.lock. Used by accept and started, later by renew and report.
    """
    # 1, no such lease -> refuse
    # 2, the lease belongs to a different Provider -> refuse
    # 3, the lease does not have the status this action needs -> refuse
    # Check 2 comes before 3, so a wrong Provider learns nothing about the lease.
    lease = store.leases.get(lease_id)
    if lease is None:
        raise LeaseNotFound(lease_id)
    if lease.provider_id != provider_id:
        raise LeaseProviderMismatch(lease_id, lease.provider_id, provider_id)
    if lease.status != expected_status:
        raise InvalidLeaseState(lease_id, lease.status.value, expected_status.value)
    # Return the real lease, not a copy, because the caller is going to change it.
    return lease


async def accept_lease(
    store: Store,
    lease_id: str,
    request: LeaseActionRequest,
    received_at: datetime,
    lease_duration_seconds: float,
) -> AcceptResponse:
    """
    A Provider says yes to an offer. Its Job starts one attempt.
    """
    # Check everything first, then change. If any check fails, nothing is changed.
    # 1, no such lease -> refuse
    # 2, the offer was made to a different Provider -> refuse
    # 3, the lease is not an open offer any more -> refuse
    # 4, the offer ran out of time -> refuse
    # 5, the Provider is not ACTIVE -> refuse
    # 6, the Job is not waiting for this lease -> refuse
    # 7, all good -> lease ACTIVE, job STARTING, attempt_count + 1
    async with store.lock:
        # Cases 1-3
        lease = _find_owned_lease(store, lease_id, request.provider_id, LeaseStatus.OFFERED)

        # Case 4: at the deadline itself, the offer is already over.
        # We only refuse. The monitor will mark the lease EXPIRED.
        if received_at >= lease.offer_deadline:
            raise OfferExpired(lease_id, lease.offer_deadline)

        # Cases 5 and 6 should never happen. We keep them in case of a bug elsewhere.
        # Why never: a Provider that goes STALE or starts draining loses its offers.
        provider = store.providers[lease.provider_id]
        # Case 5
        if provider.status != ProviderStatus.ACTIVE:
            raise ProviderNotActive(provider.provider_id, provider.status.value)

        job = store.jobs[lease.job_id]
        # Case 6
        if job.status != JobStatus.QUEUED or job.current_lease_id != lease_id:
            raise LeaseNotCurrent(lease_id, job.job_id)

        # Case 7: this is the only place attempt_count goes up.
        lease.status = LeaseStatus.ACTIVE
        lease.accepted_at = received_at
        lease.expires_at = received_at + timedelta(seconds=lease_duration_seconds)
        job.status = JobStatus.STARTING
        job.attempt_count += 1

        return AcceptResponse(
            lease_id=lease.lease_id,
            lease_status=lease.status,
            expires_at=lease.expires_at,
            job_id=job.job_id,
            job_status=job.status,
            attempt_count=job.attempt_count,
        )


async def mark_lease_started(
    store: Store, lease_id: str, request: LeaseActionRequest, received_at: datetime
) -> StartedResponse:
    """
    A Provider says the Job it accepted has really started to run.
    """
    # Check everything first, then change. If any check fails, nothing is changed.
    # 1, no such lease -> refuse
    # 2, the lease belongs to a different Provider -> refuse
    # 3, the lease is not ACTIVE -> refuse
    # 4, the lease ran out of time -> refuse
    # 5, the Provider is STALE or DRAINED -> refuse
    # 6, the Job is not on this lease any more -> refuse
    # 7, the Job is not STARTING, for example started was sent twice -> refuse
    # 8, all good -> job RUNNING, lease stays ACTIVE
    async with store.lock:
        # Cases 1-3
        lease = _find_owned_lease(store, lease_id, request.provider_id, LeaseStatus.ACTIVE)

        # Case 4: same rule as the offer deadline. We only refuse, the monitor marks it.
        if received_at >= lease.expires_at:
            raise LeaseExpired(lease_id, lease.expires_at)

        # Case 5: DRAINING is allowed, because a drain only stops new offers.
        # DRAINED can still get here: finish_drains does not take leases back yet (TODO).
        provider = store.providers[lease.provider_id]
        if provider.status not in (ProviderStatus.ACTIVE, ProviderStatus.DRAINING):
            raise ProviderNotActive(provider.provider_id, provider.status.value)

        job = store.jobs[lease.job_id]
        # Case 6: should never happen. We keep it in case of a bug elsewhere.
        if job.current_lease_id != lease_id:
            raise LeaseNotCurrent(lease_id, job.job_id)

        # Case 7
        if job.status != JobStatus.STARTING:
            raise InvalidJobState(job.job_id, job.status.value, JobStatus.STARTING.value)

        # Case 8: report SUCCESS only works on a RUNNING job, so started must come first.
        lease.started_at = received_at
        job.status = JobStatus.RUNNING

        return StartedResponse(
            lease_id=lease.lease_id,
            lease_status=lease.status,
            job_id=job.job_id,
            job_status=job.status,
        )


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


async def expire_offers(
    store: Store, now: datetime, max_attempts: int, offer_timeouts_before_exclude: int
) -> None:
    """Expire unanswered offers and count timeouts for each Job/Provider pair."""
    async with store.lock:
        for lease in store.leases.values():
            if lease.status != LeaseStatus.OFFERED or now < lease.offer_deadline:
                continue

            job = store.jobs[lease.job_id]
            # Check before close_lease clears current_lease_id.
            is_current = job.current_lease_id == lease.lease_id
            close_lease(
                store,
                lease.lease_id,
                LeaseStatus.EXPIRED,
                InternalReason.OFFER_TIMEOUT.value,
                now,
                max_attempts,
            )
            if is_current:
                count = job.offer_timeout_counts.get(lease.provider_id, 0) + 1
                job.offer_timeout_counts[lease.provider_id] = count
                if count >= offer_timeouts_before_exclude:
                    job.rejected_provider_ids.add(lease.provider_id)


async def expire_execution_leases(store: Store, now: datetime, max_attempts: int) -> None:
    """Close accepted Leases that were not renewed before their deadline."""
    async with store.lock:
        for lease in store.leases.values():
            if (
                lease.status == LeaseStatus.ACTIVE
                and lease.expires_at is not None
                and now >= lease.expires_at
            ):
                close_lease(
                    store,
                    lease.lease_id,
                    LeaseStatus.EXPIRED,
                    InternalReason.EXECUTION_TIMEOUT.value,
                    now,
                    max_attempts,
                )


async def get_lease(store: Store, lease_id: str) -> Lease:
    """
    Return a copy of one Lease. Only reads, never changes anything.
    """
    # 1, no such lease -> refuse
    # 2, lease found -> return a copy, whatever its status
    async with store.lock:
        # Lock even for reading, so we never see another request's change half done.
        lease = store.leases.get(lease_id)

        # Case 1
        if lease is None:
            raise LeaseNotFound(lease_id)

        # Case 2: a copy, so whoever gets it cannot change the Store.
        return lease.model_copy(deep=True)


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


async def reject_lease(
    store: Store, lease_id: str, request: RejectRequest, now: datetime
) -> RejectResponse:
    async with store.lock:
        if lease_id not in store.leases:
            raise LeaseNotFound(lease_id)

        lease = store.leases[lease_id]

        if lease.provider_id != request.provider_id:
            raise LeaseProviderMismatch(lease_id, lease.provider_id, request.provider_id)

        if lease.status != LeaseStatus.OFFERED:
            raise InvalidLeaseState(lease_id, lease.status.value, LeaseStatus.OFFERED.value)

        job = store.jobs[lease.job_id]
        job.rejected_provider_ids.add(lease.provider_id)

        close_lease(store, lease_id, LeaseStatus.REJECTED, request.reason.value, now, 0)

        return RejectResponse(
            lease_id=lease.lease_id,
            lease_status=lease.status,
            job_status=job.status.value,
        )
