"""POST /leases/{id}/started, tested layer by layer: service -> route -> end to end."""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from coordinator.api import leases as leases_api
from coordinator.app import create_app
from coordinator.models.job import Job, JobStatus
from coordinator.models.lease import Lease, LeaseActionRequest, LeaseStatus, StartedResponse
from coordinator.models.provider import Capabilities, Provider, ProviderStatus
from coordinator.services import lease_service
from coordinator.settings import load_settings
from coordinator.store import Store

NOW = datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc)
# The lease was accepted at NOW and runs until EXPIRES_AT; started arrives 1 s in.
EXPIRES_AT = NOW + timedelta(seconds=10)
STARTED_AT = NOW + timedelta(seconds=1)


def provider(status=ProviderStatus.ACTIVE):
    return Provider(
        provider_id="p1",
        status=status,
        capabilities=Capabilities(
            gpu_model="A100", vram_mb=40960, driver_version="550.54", runtimes=["cuda12"]
        ),
        accepted_tiers=["standard"],
        registered_at=NOW,
        last_heartbeat_at=NOW,
    )


def job(status=JobStatus.STARTING, current_lease_id="l1", attempt_count=1):
    return Job(
        job_id="j1",
        status=status,
        required_vram_mb=8192,
        gpu_models=[],
        required_runtime="cuda12",
        tier="standard",
        attempt_count=attempt_count,
        current_lease_id=current_lease_id,
        created_at=NOW,
    )


def lease(status=LeaseStatus.ACTIVE, expires_at=EXPIRES_AT):
    return Lease(
        lease_id="l1",
        job_id="j1",
        provider_id="p1",
        status=status,
        offer_deadline=NOW + timedelta(seconds=5),
        expires_at=expires_at,
        created_at=NOW,
    )


def run_started(
    stored_provider=None,
    stored_job=None,
    stored_lease=None,
    lease_id="l1",
    provider_id="p1",
    received_at=STARTED_AT,
):
    """Send started for lease_id in a fresh Store holding p1, j1 and l1 (accepted by default).

    The Store gets copies, so the objects passed in stay as they were and a test can
    compare against them. Returns (store, returned response or raised exception).
    """
    seeds = [
        provider() if stored_provider is None else stored_provider,
        job() if stored_job is None else stored_job,
        lease() if stored_lease is None else stored_lease,
    ]

    async def scenario():
        # Store is created inside the event loop (asyncio.Lock on Python 3.9).
        store = Store()
        seeded_provider, seeded_job, seeded_lease = (seed.model_copy(deep=True) for seed in seeds)
        store.providers["p1"] = seeded_provider
        store.jobs["j1"] = seeded_job
        store.leases["l1"] = seeded_lease
        try:
            result = await lease_service.mark_lease_started(
                store, lease_id, LeaseActionRequest(provider_id=provider_id), received_at
            )
        except Exception as exc:
            result = exc
        return store, result

    return asyncio.run(scenario())


@pytest.fixture
def client():
    # Long loop intervals: the scheduler and monitor tick once at startup and then
    # stay out of the way, so the records a test puts in the Store are not changed.
    settings = load_settings().model_copy(
        update={"monitor_interval_seconds": 3600, "scheduler_interval_seconds": 3600}
    )
    with TestClient(create_app(clock=lambda: NOW, settings=settings)) as test_client:
        yield test_client


# --- Service layer -----------------------------------------------------------


@pytest.mark.parametrize(
    "provider_status",
    [ProviderStatus.ACTIVE, ProviderStatus.DRAINING],
    ids=["provider-active", "provider-draining"],
)
def test_service_started_marks_job_running(provider_status):
    # A drain only stops new offers, so work accepted before it may still start.
    store, returned = run_started(stored_provider=provider(provider_status))

    assert returned == StartedResponse(
        lease_id="l1", lease_status=LeaseStatus.ACTIVE, job_id="j1", job_status=JobStatus.RUNNING
    )
    stored_lease = store.leases["l1"]
    assert stored_lease.status is LeaseStatus.ACTIVE
    assert stored_lease.started_at == STARTED_AT
    # started does not extend the lease; only renew does.
    assert stored_lease.expires_at == EXPIRES_AT
    # The attempt was already counted at accept.
    assert store.jobs["j1"] == job(JobStatus.RUNNING)
    assert store.providers["p1"] == provider(provider_status)


@pytest.mark.parametrize(
    "received_at, expected",
    [
        (EXPIRES_AT - timedelta(microseconds=1), StartedResponse),
        (EXPIRES_AT, lease_service.LeaseExpired),
        (EXPIRES_AT + timedelta(seconds=1), lease_service.LeaseExpired),
    ],
    ids=["just-before-deadline", "at-deadline", "after-deadline"],
)
def test_service_deadline_moment_already_counts_as_expired(received_at, expected):
    # Same rule as the offer deadline: once the deadline is reached, it is over.
    _, result = run_started(received_at=received_at)

    assert isinstance(result, expected)


@pytest.mark.parametrize(
    "case, expected",
    [
        ({"lease_id": "no-such-lease"}, lease_service.LeaseNotFound),
        ({"provider_id": "p2"}, lease_service.LeaseProviderMismatch),
        (
            {
                "stored_lease": lease(LeaseStatus.OFFERED, expires_at=None),
                "stored_job": job(JobStatus.QUEUED, attempt_count=0),
            },
            lease_service.InvalidLeaseState,
        ),
        (
            {
                "stored_lease": lease(LeaseStatus.REVOKED),
                "stored_job": job(JobStatus.QUEUED, current_lease_id=None),
            },
            lease_service.InvalidLeaseState,
        ),
        ({"received_at": EXPIRES_AT}, lease_service.LeaseExpired),
        # Should never happen, because the monitor takes the lease back first. We still refuse.
        ({"stored_provider": provider(ProviderStatus.DRAINED)}, lease_service.ProviderNotActive),
        ({"stored_provider": provider(ProviderStatus.STALE)}, lease_service.ProviderNotActive),
        ({"stored_job": job(current_lease_id="l2")}, lease_service.LeaseNotCurrent),
        ({"stored_job": job(JobStatus.RUNNING)}, lease_service.InvalidJobState),
    ],
    ids=[
        "unknown-lease",
        "other-provider",
        "not-accepted-yet",
        "lease-ended",
        "lease-expired",
        "provider-drained",
        "provider-stale",
        "job-moved-on",
        "already-started",
    ],
)
def test_service_refused_started_changes_nothing(case, expected):
    # The default Store contents, with this case's changes on top.
    seeds = {"stored_provider": provider(), "stored_job": job(), "stored_lease": lease(), **case}

    store, raised = run_started(**seeds)

    assert isinstance(raised, expected)
    # Checks run before any change, because the Store has no rollback.
    assert store.providers["p1"] == seeds["stored_provider"]
    assert store.jobs["j1"] == seeds["stored_job"]
    assert store.leases["l1"] == seeds["stored_lease"]


# --- API layer: the service is replaced by a fake ----------------------------


@pytest.mark.parametrize(
    "exception, status_code, error_code",
    [
        (lease_service.LeaseNotFound("l1"), 404, "LEASE_NOT_FOUND"),
        (lease_service.LeaseProviderMismatch("l1", "p1", "p2"), 403, "LEASE_PROVIDER_MISMATCH"),
        (lease_service.InvalidLeaseState("l1", "OFFERED", "ACTIVE"), 409, "INVALID_LEASE_STATE"),
        (lease_service.LeaseExpired("l1", EXPIRES_AT), 409, "LEASE_EXPIRED"),
        (lease_service.ProviderNotActive("p1", "DRAINED"), 409, "PROVIDER_NOT_ACTIVE"),
        (lease_service.LeaseNotCurrent("l1", "j1"), 409, "LEASE_NOT_CURRENT"),
        (lease_service.InvalidJobState("j1", "RUNNING", "STARTING"), 409, "INVALID_JOB_STATE"),
    ],
    ids=[
        "not-found",
        "other-provider",
        "not-active",
        "lease-expired",
        "provider-not-active",
        "not-current",
        "job-not-starting",
    ],
)
def test_route_translates_service_errors(client, monkeypatch, exception, status_code, error_code):
    async def fake_started(*args):
        raise exception

    # The route imports mark_lease_started by name, so replace it where the route looks it up.
    monkeypatch.setattr(leases_api, "mark_lease_started", fake_started)

    response = client.post("/leases/l1/started", json={"provider_id": "p1"})

    assert response.status_code == status_code
    assert response.json()["error"]["code"] == error_code


@pytest.mark.parametrize(
    "body",
    [{}, {"provider_id": ""}, {"provider_id": "p1", "outcome": "SUCCESS"}],
    ids=["no-provider-id", "empty-provider-id", "unknown-field"],
)
def test_route_rejects_bad_body_before_the_service(client, monkeypatch, body):
    async def fake_started(*args):
        pytest.fail("Invalid input must not reach the service")

    monkeypatch.setattr(leases_api, "mark_lease_started", fake_started)

    response = client.post("/leases/l1/started", json=body)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


# --- End to end --------------------------------------------------------------


def test_e2e_accept_then_started_twice(client):
    store = client.app.state.store
    store.providers["p1"] = provider()
    store.jobs["j1"] = job(JobStatus.QUEUED, attempt_count=0)
    store.leases["l1"] = lease(LeaseStatus.OFFERED, expires_at=None)

    accepted = client.post("/leases/l1/accept", json={"provider_id": "p1"})
    started = client.post("/leases/l1/started", json={"provider_id": "p1"})
    started_again = client.post("/leases/l1/started", json={"provider_id": "p1"})

    assert accepted.status_code == 200
    assert started.status_code == 200
    # The contract's response shape.
    assert started.json() == {
        "lease_id": "l1",
        "lease_status": "ACTIVE",
        "job_id": "j1",
        "job_status": "RUNNING",
    }
    assert client.get("/leases/l1").json()["started_at"] == "2026-11-05T12:00:00Z"
    # A repeated started is refused: the Job is already RUNNING.
    assert started_again.status_code == 409
    assert started_again.json()["error"]["code"] == "INVALID_JOB_STATE"
