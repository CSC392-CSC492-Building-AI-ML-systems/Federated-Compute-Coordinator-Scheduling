from datetime import datetime

from coordinator.models.job import JobStatus
from coordinator.models.lease import TERMINAL_LEASE_STATUSES, InternalReason, LeaseStatus
from coordinator.models.provider import (
    HeartbeatRequest,
    Provider,
    ProviderRegisterRequest,
    ProviderStatus,
)
from coordinator.store import Store


class ProviderAlreadyExists(Exception):
    """The submitted provider_id is already registered.

    The service only reports the fact; the API layer decides it means HTTP 409.
    """

    def __init__(self, provider_id: str):
        super().__init__(f"provider_id {provider_id!r} already exists")
        self.provider_id = provider_id


async def register_provider(
    store: Store, request: ProviderRegisterRequest, received_at: datetime
) -> Provider:
    """Register a provider and return a copy of the stored record.

    received_at is the Coordinator's receive time, passed in by the caller so tests
    can use a fixed clock and the service never reads the clock itself.
    """
    new_provider = Provider(
        provider_id=request.provider_id,
        status=ProviderStatus.ACTIVE,
        capabilities=request.capabilities.model_copy(deep=True),
        accepted_tiers=list(request.accepted_tiers),
        registered_at=received_at,
        # Registration counts as the first contact, so both timestamps match.
        last_heartbeat_at=received_at,
    )

    # Check and insert under the same lock hold; otherwise two concurrent requests
    # with the same ID could both pass the check.
    async with store.lock:
        if request.provider_id in store.providers:
            # Never overwrite: a duplicate must not replace a live record.
            raise ProviderAlreadyExists(request.provider_id)
        store.providers[new_provider.provider_id] = new_provider
        # Return a copy so callers cannot change shared state without the lock.
        return new_provider.model_copy(deep=True)


class ProviderNotFound(Exception):
    """The requested Provider does not exist; routes translate this to HTTP 404."""

    def __init__(self, provider_id: str):
        super().__init__(f"provider_id {provider_id!r} not found")
        self.provider_id = provider_id


class ProviderDrained(Exception):
    """A DRAINED Provider cannot heartbeat; routes translate this to HTTP 409."""

    def __init__(self, provider_id: str):
        super().__init__(f"provider_id {provider_id!r} is DRAINED")
        self.provider_id = provider_id


async def heartbeat(
    store: Store, provider_id: str, request: HeartbeatRequest, received_at: datetime
) -> Provider:
    """
    Record a heartbeat and return a copy of the updated Provider.
    ProviderDrained for DRAINED without changing the stored record. Otherwise set
    last_heartbeat_at to received_at; STALE -> ACTIVE, ACTIVE/DRAINING unchanged.
    Revoked Leases stay terminal. request.provider_time is for logging only.
    Return a deep copy, as register_provider does.
    """
    async with store.lock:
        provider = store.providers.get(provider_id)
        if provider is None:
            raise ProviderNotFound(provider_id)

        if provider.status == ProviderStatus.DRAINED:
            raise ProviderDrained(provider_id)

        if provider.status == ProviderStatus.STALE:
            provider.status = ProviderStatus.ACTIVE

        # return a copy
        provider.last_heartbeat_at = received_at
        return provider.model_copy(deep=True)


async def expire_heartbeats(store: Store, now: datetime, timeout_seconds: float) -> None:
    """Monitor check: Providers whose last heartbeat is older than the timeout.

    ACTIVE -> STALE; DRAINING -> DRAINED (new rule, see docs/provider-tasks).
    Revoke live Leases with reason PROVIDER_STALE and requeue their current Jobs.
    Requeueing leaves attempt_count unchanged; retry budgets are not implemented.
    """
    async with store.lock:
        expired_provider_ids = set()
        for provider in store.providers.values():
            if (
                provider.status in (ProviderStatus.DRAINING, ProviderStatus.ACTIVE)
                and (now - provider.last_heartbeat_at).total_seconds() > timeout_seconds
            ):
                provider.status = (
                    ProviderStatus.STALE
                    if provider.status == ProviderStatus.ACTIVE
                    else ProviderStatus.DRAINED
                )
                expired_provider_ids.add(provider.provider_id)

        if not expired_provider_ids:
            return

        for lease in store.leases.values():
            if lease.provider_id in expired_provider_ids and lease.status in (
                LeaseStatus.OFFERED,
                LeaseStatus.ACTIVE,
            ):
                lease.status = LeaseStatus.REVOKED
                lease.reason = InternalReason.PROVIDER_STALE.value
                job = store.jobs[lease.job_id]
                if job.current_lease_id == lease.lease_id:
                    job.current_lease_id = None
                    if job.status in (JobStatus.STARTING, JobStatus.RUNNING):
                        job.last_failure_reason = lease.reason
                        job.status = JobStatus.QUEUED


def _has_live_lease(store: Store, provider_id: str) -> bool:
    # Caller holds store.lock.
    for lease in store.leases.values():
        if lease.provider_id == provider_id and lease.status not in TERMINAL_LEASE_STATUSES:
            return True
    return False


async def finish_drains(store: Store, now: datetime) -> None:
    """Monitor check: DRAINING Providers with no live Lease, or past drain_deadline.
    DRAINING -> DRAINED. Later: reclaim remaining Leases with reason DRAIN_RECLAIM.

    In other words, check draining -> drain or continue
    """
    async with store.lock:
        for provider in store.providers.values():
            if provider.status != ProviderStatus.DRAINING:
                continue

            if provider.drain_deadline is not None:
                past_deadline = now >= provider.drain_deadline

                if _has_live_lease(store, provider.provider_id) and not past_deadline:
                    continue

            # TODO: when past_deadline, revoke remaining Leases (DRAIN_RECLAIM)
            # and send their Jobs to the retry check before marking DRAINEDD. =)
            provider.status = ProviderStatus.DRAINED
