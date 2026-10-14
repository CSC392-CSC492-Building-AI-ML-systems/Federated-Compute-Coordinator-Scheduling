"""GET /status, tested layer by layer: service -> route -> end to end."""

import asyncio
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.models.job import Job, JobStatus
from coordinator.models.lease import Lease, LeaseStatus
from coordinator.models.provider import Capabilities, Provider, ProviderStatus
from coordinator.models.status import StatusSnapshot
from coordinator.services import status_service
from coordinator.settings import load_settings
from coordinator.store import Store

NOW = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)

# The contract's shape (section 11): every state listed, 0 when nobody is in it.
EMPTY = {
    "providers": {"ACTIVE": 0, "STALE": 0, "DRAINING": 0, "DRAINED": 0},
    "jobs": {"QUEUED": 0, "STARTING": 0, "RUNNING": 0, "COMPLETED": 0, "FAILED": 0},
    "leases": {
        "OFFERED": 0,
        "ACTIVE": 0,
        "REJECTED": 0,
        "EXPIRED": 0,
        "REVOKED": 0,
        "RELEASED": 0,
    },
}


def provider(provider_id, status):
    return Provider(
        provider_id=provider_id,
        status=status,
        capabilities=Capabilities(
            gpu_model="A100", vram_mb=40960, driver_version="550.54", runtimes=["cuda12"]
        ),
        accepted_tiers=["standard"],
        registered_at=NOW,
        last_heartbeat_at=NOW,
    )


def job(job_id, status):
    return Job(
        job_id=job_id,
        status=status,
        required_vram_mb=8192,
        gpu_models=[],
        required_runtime="cuda12",
        tier="standard",
        created_at=NOW,
    )


def lease(lease_id, status):
    return Lease(
        lease_id=lease_id,
        job_id="j1",
        provider_id="p1",
        status=status,
        offer_deadline=NOW,
        created_at=NOW,
    )


def filled_store():
    """Call inside a coroutine: Store needs a running event loop on Python 3.9."""
    store = Store()
    for record in (
        provider("p1", ProviderStatus.ACTIVE),
        provider("p2", ProviderStatus.ACTIVE),
        provider("p3", ProviderStatus.STALE),
        provider("p4", ProviderStatus.DRAINED),
    ):
        store.providers[record.provider_id] = record
    for record in (job("j1", JobStatus.RUNNING), job("j2", JobStatus.QUEUED)):
        store.jobs[record.job_id] = record
    for record in (lease("l1", LeaseStatus.ACTIVE), lease("l0", LeaseStatus.REJECTED)):
        store.leases[record.lease_id] = record
    return store


def dump(store):
    return (
        {key: value.model_dump() for key, value in store.providers.items()},
        {key: value.model_dump() for key, value in store.jobs.items()},
        {key: value.model_dump() for key, value in store.leases.items()},
    )


@pytest.fixture
def client():
    # Long loop intervals: the scheduler and monitor tick once at startup (empty
    # Store) and then stay out of the way, so the counts below are exact.
    settings = load_settings().model_copy(
        update={"monitor_interval_seconds": 3600, "scheduler_interval_seconds": 3600}
    )
    with TestClient(create_app(clock=lambda: NOW, settings=settings)) as test_client:
        yield test_client


# --- Service layer -----------------------------------------------------------


def test_service_lists_every_state_as_zero_for_an_empty_store():
    async def scenario():
        return await status_service.get_status(Store())

    assert asyncio.run(scenario()).model_dump() == EMPTY


def test_service_counts_records_by_state():
    async def scenario():
        return await status_service.get_status(filled_store())

    snapshot = asyncio.run(scenario())

    assert snapshot.providers == {"ACTIVE": 2, "STALE": 1, "DRAINING": 0, "DRAINED": 1}
    assert snapshot.jobs == {
        "QUEUED": 1,
        "STARTING": 0,
        "RUNNING": 1,
        "COMPLETED": 0,
        "FAILED": 0,
    }
    assert snapshot.leases == {**EMPTY["leases"], "ACTIVE": 1, "REJECTED": 1}


def test_service_is_read_only():
    async def scenario():
        store = filled_store()
        before = dump(store)
        await status_service.get_status(store)
        return before, dump(store)

    before, after = asyncio.run(scenario())

    assert after == before


# --- API layer: the service is replaced by a fake ----------------------------


def test_route_returns_what_the_service_reports(client, monkeypatch):
    calls = []
    canned = {**EMPTY, "providers": {**EMPTY["providers"], "ACTIVE": 3}}

    async def fake_get_status(store):
        calls.append(store)
        return StatusSnapshot(**canned)

    monkeypatch.setattr(status_service, "get_status", fake_get_status)

    response = client.get("/status")

    assert calls == [client.app.state.store]
    assert response.status_code == 200
    assert response.json() == canned


# --- End to end --------------------------------------------------------------


def test_e2e_status_follows_registrations_and_submissions(client):
    assert client.get("/status").json() == EMPTY

    client.post(
        "/providers",
        json={
            "provider_id": "provider-a",
            "capabilities": {
                "gpu_model": "A100",
                "vram_mb": 40960,
                "driver_version": "550.54",
                "runtimes": ["cuda12"],
            },
            "accepted_tiers": ["standard"],
        },
    )
    client.post(
        "/jobs",
        json={"required_vram_mb": 8192, "required_runtime": "cuda12", "tier": "standard"},
    )

    body = client.get("/status").json()

    assert body["providers"] == {**EMPTY["providers"], "ACTIVE": 1}
    assert body["jobs"] == {**EMPTY["jobs"], "QUEUED": 1}
    assert body["leases"] == EMPTY["leases"]
