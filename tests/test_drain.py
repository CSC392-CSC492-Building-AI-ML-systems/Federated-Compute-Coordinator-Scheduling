"""POST /providers/{id}/drain, tested layer by layer: service -> route -> end to end."""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.models.job import Job, JobStatus
from coordinator.models.lease import Lease, LeaseStatus
from coordinator.models.provider import Capabilities, DrainRequest, Provider, ProviderStatus
from coordinator.services import provider_service
from coordinator.settings import load_settings
from coordinator.store import Store

NOW = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)
GRACE = DrainRequest(grace_period_seconds=60)


def provider(status, drain_deadline=None):
    return Provider(
        provider_id="p1",
        status=status,
        capabilities=Capabilities(
            gpu_model="A100", vram_mb=40960, driver_version="550.54", runtimes=["cuda12"]
        ),
        accepted_tiers=["standard"],
        registered_at=NOW,
        last_heartbeat_at=NOW,
        drain_deadline=drain_deadline,
    )


def job(job_id, status, lease_id):
    return Job(
        job_id=job_id,
        status=status,
        required_vram_mb=8192,
        gpu_models=[],
        required_runtime="cuda12",
        tier="standard",
        current_lease_id=lease_id,
        created_at=NOW,
    )


def lease(lease_id, job_id, status):
    return Lease(
        lease_id=lease_id,
        job_id=job_id,
        provider_id="p1",
        status=status,
        offer_deadline=NOW,
        created_at=NOW,
    )


def run_drain(status, request=GRACE, received_at=NOW, drain_deadline=None):
    """Drain p1 in a fresh Store. Returns (store, returned record or raised exception)."""

    async def scenario():
        # Store is created inside the event loop (asyncio.Lock on Python 3.9).
        store = Store()
        if status is not None:
            store.providers["p1"] = provider(status, drain_deadline)
        try:
            result = await provider_service.drain(store, "p1", request, received_at)
        except Exception as exc:
            result = exc
        return store, result

    return asyncio.run(scenario())


@pytest.fixture
def client():
    # Long loop intervals: the monitor ticks once at startup and then stays out of
    # the way, so a DRAINING Provider is not turned DRAINED in the middle of a test.
    settings = load_settings().model_copy(
        update={"monitor_interval_seconds": 3600, "scheduler_interval_seconds": 3600}
    )
    with TestClient(create_app(clock=lambda: NOW, settings=settings)) as test_client:
        yield test_client


# --- Service layer -----------------------------------------------------------


@pytest.mark.parametrize("status", [ProviderStatus.ACTIVE, ProviderStatus.STALE])
def test_service_starts_draining_with_deadline_from_coordinator_time(status):
    store, returned = run_drain(status)

    stored = store.providers["p1"]
    assert stored.status is ProviderStatus.DRAINING
    assert stored.drain_deadline == NOW + timedelta(seconds=60)
    assert returned == stored
    assert returned is not stored  # a copy, not the shared record


def test_service_repeat_drain_does_not_extend_the_deadline():
    first_deadline = NOW + timedelta(seconds=60)

    store, returned = run_drain(
        ProviderStatus.DRAINING,
        request=DrainRequest(grace_period_seconds=600),
        received_at=NOW + timedelta(seconds=30),
        drain_deadline=first_deadline,
    )

    assert returned.status is ProviderStatus.DRAINING
    assert store.providers["p1"].drain_deadline == first_deadline


@pytest.mark.parametrize(
    "status, expected",
    [
        (ProviderStatus.DRAINED, provider_service.ProviderDrained),
        (None, provider_service.ProviderNotFound),
    ],
    ids=["drained", "unknown-id"],
)
def test_service_rejects_drained_or_unknown_provider(status, expected):
    store, raised = run_drain(status)

    assert isinstance(raised, expected)
    if status is not None:
        assert store.providers["p1"] == provider(status)  # unchanged


def test_service_revokes_open_offers_but_keeps_running_leases():
    async def scenario():
        store = Store()
        store.providers["p1"] = provider(ProviderStatus.ACTIVE)
        store.jobs["j-offered"] = job("j-offered", JobStatus.QUEUED, "l-offered")
        store.jobs["j-running"] = job("j-running", JobStatus.RUNNING, "l-running")
        store.leases["l-offered"] = lease("l-offered", "j-offered", LeaseStatus.OFFERED)
        store.leases["l-running"] = lease("l-running", "j-running", LeaseStatus.ACTIVE)
        await provider_service.drain(store, "p1", GRACE, NOW)
        return store

    store = asyncio.run(scenario())

    # The unanswered offer goes back to the queue without using an attempt.
    assert store.leases["l-offered"].status is LeaseStatus.REVOKED
    assert store.leases["l-offered"].reason == "DRAIN_STARTED"
    assert store.jobs["j-offered"].status is JobStatus.QUEUED
    assert store.jobs["j-offered"].current_lease_id is None
    assert store.jobs["j-offered"].attempt_count == 0
    # Work already accepted keeps running until it finishes or the deadline passes.
    assert store.leases["l-running"].status is LeaseStatus.ACTIVE
    assert store.jobs["j-running"].current_lease_id == "l-running"


# --- API layer: the service is replaced by a fake ----------------------------


@pytest.mark.parametrize(
    "exception, status_code, error_code",
    [
        (provider_service.ProviderNotFound, 404, "NOT_FOUND"),
        (provider_service.ProviderDrained, 409, "INVALID_STATE_TRANSITION"),
    ],
)
def test_route_translates_service_errors(client, monkeypatch, exception, status_code, error_code):
    async def fake_drain(store, provider_id, request, received_at):
        raise exception(provider_id)

    monkeypatch.setattr(provider_service, "drain", fake_drain)

    response = client.post("/providers/p1/drain", json={"grace_period_seconds": 60})

    assert response.status_code == status_code
    assert response.json()["error"]["code"] == error_code


@pytest.mark.parametrize("body", [{"grace_period_seconds": 0}, {}, None])
def test_route_rejects_bad_body_before_the_service(client, monkeypatch, body):
    async def fake_drain(*args):
        pytest.fail("Invalid input must not reach the service")

    monkeypatch.setattr(provider_service, "drain", fake_drain)

    response = client.post("/providers/p1/drain", json=body)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


# --- End to end --------------------------------------------------------------


def test_e2e_register_then_drain_twice(client):
    client.post(
        "/providers",
        json={
            "provider_id": "p1",
            "capabilities": {
                "gpu_model": "A100",
                "vram_mb": 40960,
                "driver_version": "550.54",
                "runtimes": ["cuda12"],
            },
            "accepted_tiers": ["standard"],
        },
    )
    expected = {
        "provider_id": "p1",
        "status": "DRAINING",
        "drain_deadline": "2026-10-13T12:01:00Z",
    }

    first = client.post("/providers/p1/drain", json={"grace_period_seconds": 60})
    second = client.post("/providers/p1/drain", json={"grace_period_seconds": 600})

    assert first.status_code == 200
    assert first.json() == expected
    assert second.status_code == 200
    assert second.json() == expected  # the longer grace period did not move the deadline
