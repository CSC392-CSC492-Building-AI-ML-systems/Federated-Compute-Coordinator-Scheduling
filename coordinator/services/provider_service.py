"""Provider use cases. Names and signatures are placeholders for team design."""

from datetime import datetime

from coordinator.models.provider import Provider, ProviderRegisterRequest, ProviderStatus
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


async def heartbeat(provider_id):
    """Record a provider heartbeat."""
    raise NotImplementedError("Team implementation pending")


async def expire_heartbeats(store: Store, now: datetime, timeout_seconds: float) -> None:
    """Monitor check: Providers whose last heartbeat is older than the timeout.

    ACTIVE -> STALE; DRAINING -> DRAINED (new rule, see docs/provider-tasks).
    Later: also revoke their Leases with reason PROVIDER_STALE.
    """
    # TODO(Jingcheng): implement under store.lock. Called by monitor.run_monitor_tick.
    return None


async def finish_drains(store: Store, now: datetime) -> None:
    """Monitor check: DRAINING Providers with no live Lease, or past drain_deadline.

    DRAINING -> DRAINED. Later: reclaim remaining Leases with reason DRAIN_RECLAIM.
    """
    # TODO(Jacky): implement under store.lock. Called by monitor.run_monitor_tick.
    return None
