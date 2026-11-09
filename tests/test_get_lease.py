"""GET /leases/{id}, tested layer by layer: service -> end to end."""

import asyncio
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.models.lease import Lease, LeaseStatus
from coordinator.services import lease_service
from coordinator.settings import load_settings
from coordinator.store import Store

NOW = datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc)


def lease(status):
    return Lease(
        lease_id="l1",
        job_id="j1",
        provider_id="p1",
        status=status,
        offer_deadline=NOW,
        created_at=NOW,
    )


def run_get(stored_lease, lease_id="l1"):
    """Read lease_id from a fresh Store. Returns (store, returned record or raised exception)."""

    async def scenario():
        # Store is created inside the event loop (asyncio.Lock on Python 3.9).
        store = Store()
        if stored_lease is not None:
            store.leases[stored_lease.lease_id] = stored_lease
        try:
            result = await lease_service.get_lease(store, lease_id)
        except Exception as exc:
            result = exc
        return store, result

    return asyncio.run(scenario())


@pytest.fixture
def client():
    # Long loop intervals: the scheduler and monitor tick once at startup and then
    # stay out of the way, so the Lease a test puts in the Store is not changed.
    settings = load_settings().model_copy(
        update={"monitor_interval_seconds": 3600, "scheduler_interval_seconds": 3600}
    )
    with TestClient(create_app(clock=lambda: NOW, settings=settings)) as test_client:
        yield test_client


# --- Service layer -----------------------------------------------------------


@pytest.mark.parametrize("status", list(LeaseStatus), ids=lambda status: status.value)
def test_service_returns_a_copy_of_the_lease_in_any_status(status):
    # Ended Leases are readable too: that is how you find out why a Job failed.
    store, returned = run_get(lease(status))

    assert returned == lease(status)
    assert returned is not store.leases["l1"]  # a copy, not the shared record

    # Changing the copy must not change the Store.
    returned.reason = "CHANGED_BY_CALLER"
    assert store.leases["l1"] == lease(status)


def test_service_raises_for_unknown_lease():
    store, raised = run_get(lease(LeaseStatus.ACTIVE), lease_id="no-such-lease")

    assert isinstance(raised, lease_service.LeaseNotFound)
    assert raised.lease_id == "no-such-lease"
    assert store.leases == {"l1": lease(LeaseStatus.ACTIVE)}  # read-only


# --- End to end --------------------------------------------------------------


def test_e2e_returns_the_full_lease_record(client):
    client.app.state.store.leases["l1"] = lease(LeaseStatus.ACTIVE)

    response = client.get("/leases/l1")

    assert response.status_code == 200
    # Every field survives the trip through JSON.
    assert Lease.model_validate(response.json()) == lease(LeaseStatus.ACTIVE)


def test_e2e_unknown_lease_is_404(client):
    response = client.get("/leases/no-such-lease")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "LEASE_NOT_FOUND"
