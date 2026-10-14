"""finish_drains: when does a DRAINING Provider become DRAINED?"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from coordinator.models.lease import Lease, LeaseStatus
from coordinator.models.provider import Capabilities, Provider, ProviderStatus
from coordinator.services import provider_service
from coordinator.store import Store

NOW = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)


def draining_provider(deadline):
    return Provider(
        provider_id="p1",
        status=ProviderStatus.DRAINING,
        capabilities=Capabilities(
            gpu_model="A100", vram_mb=40960, driver_version="550.54", runtimes=["cuda12"]
        ),
        accepted_tiers=["tier1"],
        registered_at=NOW,
        last_heartbeat_at=NOW,
        drain_deadline=deadline,
    )


def lease_on_p1(status):
    return Lease(
        lease_id="l1",
        job_id="j1",
        provider_id="p1",
        status=status,
        offer_deadline=NOW,
        created_at=NOW,
    )


def run_finish_drains(lease_status, now):
    """Put one DRAINING Provider (deadline NOW + 60s) and one Lease in a fresh Store."""

    async def scenario():
        # Store is created inside the event loop (asyncio.Lock on Python 3.9).
        store = Store()
        store.providers["p1"] = draining_provider(deadline=NOW + timedelta(seconds=60))
        store.leases["l1"] = lease_on_p1(lease_status)
        await provider_service.finish_drains(store, now)
        return store.providers["p1"].status

    return asyncio.run(scenario())


def test_keeps_draining_while_lease_is_live_and_deadline_not_reached():
    assert run_finish_drains(LeaseStatus.ACTIVE, now=NOW) is ProviderStatus.DRAINING


@pytest.mark.parametrize(
    "lease_status, now",
    [
        (LeaseStatus.RELEASED, NOW),  # work finished early
        (LeaseStatus.ACTIVE, NOW + timedelta(seconds=61)),  # deadline passed
    ],
    ids=["work-finished", "deadline-passed"],
)
def test_becomes_drained_when_work_is_done_or_deadline_passed(lease_status, now):
    assert run_finish_drains(lease_status, now) is ProviderStatus.DRAINED
