"""POST /leases/{id}/accept, tested layer by layer: service -> route -> end to end."""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from coordinator.api import leases as leases_api
from coordinator.app import create_app
from coordinator.models.job import Job, JobStatus
from coordinator.models.lease import AcceptResponse, Lease, LeaseActionRequest, LeaseStatus
from coordinator.models.provider import Capabilities, Provider, ProviderStatus
from coordinator.services import lease_service
from coordinator.settings import load_settings
from coordinator.store import Store

NOW = datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc)
# The offer was made at NOW with a 5 s window; the Provider answers 2 s in.
OFFER_DEADLINE = NOW + timedelta(seconds=5)
ACCEPTED_AT = NOW + timedelta(seconds=2)
LEASE_DURATION = 10


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


def job(status=JobStatus.QUEUED, current_lease_id="l1", attempt_count=0):
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


def lease(status=LeaseStatus.OFFERED):
    return Lease(
        lease_id="l1",
        job_id="j1",
        provider_id="p1",
        status=status,
        offer_deadline=OFFER_DEADLINE,
        created_at=NOW,
    )


def run_accept(
    stored_provider=None,
    stored_job=None,
    stored_lease=None,
    lease_id="l1",
    provider_id="p1",
    received_at=ACCEPTED_AT,
):
    """Accept lease_id in a fresh Store holding p1, j1 and l1 (an open offer by default).

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
            result = await lease_service.accept_lease(
                store,
                lease_id,
                LeaseActionRequest(provider_id=provider_id),
                received_at,
                LEASE_DURATION,
            )
        except Exception as exc:
            result = exc
        return store, result

    return asyncio.run(scenario())


@pytest.fixture
def client():
    # Long loop intervals: the scheduler and monitor tick once at startup and then
    # stay out of the way. A lease duration unlike the YAML value shows the route
    # really passes the configured number.
    settings = load_settings().model_copy(
        update={
            "monitor_interval_seconds": 3600,
            "scheduler_interval_seconds": 3600,
            "lease_duration_seconds": 42,
        }
    )
    with TestClient(create_app(clock=lambda: NOW, settings=settings)) as test_client:
        yield test_client


# --- Service layer -----------------------------------------------------------


@pytest.mark.parametrize(
    "attempts_before",
    [0, 2],
    ids=["first-attempt", "third-attempt-after-two-failures"],
)
def test_service_accept_starts_exactly_one_attempt(attempts_before):
    store, returned = run_accept(stored_job=job(attempt_count=attempts_before))

    # Counted from when the Coordinator received the accept, not from the offer.
    expires_at = ACCEPTED_AT + timedelta(seconds=LEASE_DURATION)
    assert returned == AcceptResponse(
        lease_id="l1",
        lease_status=LeaseStatus.ACTIVE,
        expires_at=expires_at,
        job_id="j1",
        job_status=JobStatus.STARTING,
        attempt_count=attempts_before + 1,
    )
    stored_lease = store.leases["l1"]
    assert stored_lease.status is LeaseStatus.ACTIVE
    assert stored_lease.accepted_at == ACCEPTED_AT
    assert stored_lease.expires_at == expires_at
    # The Job keeps this lease as current while it runs (invariant 6: one attempt).
    assert store.jobs["j1"] == job(
        JobStatus.STARTING, current_lease_id="l1", attempt_count=attempts_before + 1
    )
    # Provider unchanged: the now-ACTIVE lease keeps its capacity reserved.
    assert store.providers["p1"] == provider()


@pytest.mark.parametrize(
    "received_at, expected",
    [
        (OFFER_DEADLINE - timedelta(microseconds=1), AcceptResponse),
        (OFFER_DEADLINE, lease_service.OfferExpired),
        (OFFER_DEADLINE + timedelta(seconds=1), lease_service.OfferExpired),
    ],
    ids=["just-before-deadline", "at-deadline", "after-deadline"],
)
def test_service_deadline_moment_already_counts_as_expired(received_at, expected):
    # Same rule as the monitor's offer expiry: once the deadline is reached, it is over.
    _, result = run_accept(received_at=received_at)

    assert isinstance(result, expected)


@pytest.mark.parametrize(
    "case, expected",
    [
        ({"lease_id": "no-such-lease"}, lease_service.LeaseNotFound),
        ({"provider_id": "p2"}, lease_service.LeaseProviderMismatch),
        # Ownership is checked first, so p2 learns nothing about p1's expired offer.
        (
            {"provider_id": "p2", "received_at": OFFER_DEADLINE},
            lease_service.LeaseProviderMismatch,
        ),
        (
            {
                "stored_lease": lease(LeaseStatus.ACTIVE),
                "stored_job": job(JobStatus.STARTING, attempt_count=1),
            },
            lease_service.InvalidLeaseState,
        ),
        (
            {"stored_lease": lease(LeaseStatus.REJECTED), "stored_job": job(current_lease_id=None)},
            lease_service.InvalidLeaseState,
        ),
        ({"received_at": OFFER_DEADLINE}, lease_service.OfferExpired),
        ({"stored_provider": provider(ProviderStatus.DRAINING)}, lease_service.ProviderNotActive),
        ({"stored_provider": provider(ProviderStatus.STALE)}, lease_service.ProviderNotActive),
        ({"stored_job": job(current_lease_id="l2")}, lease_service.LeaseNotCurrent),
        ({"stored_job": job(JobStatus.FAILED)}, lease_service.LeaseNotCurrent),
    ],
    ids=[
        "unknown-lease",
        "other-provider",
        "other-provider-and-expired",
        "already-accepted",
        "offer-ended",
        "offer-expired",
        "provider-draining",
        "provider-stale",
        "job-moved-on",
        "job-not-queued",
    ],
)
def test_service_refused_accept_changes_nothing(case, expected):
    # The default Store contents, with this case's changes on top.
    seeds = {"stored_provider": provider(), "stored_job": job(), "stored_lease": lease(), **case}

    store, raised = run_accept(**seeds)

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
        (lease_service.InvalidLeaseState("l1", "ACTIVE", "OFFERED"), 409, "INVALID_LEASE_STATE"),
        (lease_service.OfferExpired("l1", OFFER_DEADLINE), 409, "OFFER_EXPIRED"),
        (lease_service.ProviderNotActive("p1", "DRAINING"), 409, "PROVIDER_NOT_ACTIVE"),
        (lease_service.LeaseNotCurrent("l1", "j1"), 409, "LEASE_NOT_CURRENT"),
    ],
    ids=[
        "not-found",
        "other-provider",
        "not-offered",
        "offer-expired",
        "provider-not-active",
        "not-current",
    ],
)
def test_route_translates_service_errors(client, monkeypatch, exception, status_code, error_code):
    async def fake_accept(*args):
        raise exception

    # The route imports accept_lease by name, so replace it where the route looks it up.
    monkeypatch.setattr(leases_api, "accept_lease", fake_accept)

    response = client.post("/leases/l1/accept", json={"provider_id": "p1"})

    assert response.status_code == status_code
    assert response.json()["error"]["code"] == error_code


@pytest.mark.parametrize(
    "body",
    [{}, {"provider_id": ""}, {"provider_id": "p1", "extend_seconds": 60}],
    ids=["no-provider-id", "empty-provider-id", "unknown-field"],
)
def test_route_rejects_bad_body_before_the_service(client, monkeypatch, body):
    async def fake_accept(*args):
        pytest.fail("Invalid input must not reach the service")

    monkeypatch.setattr(leases_api, "accept_lease", fake_accept)

    response = client.post("/leases/l1/accept", json=body)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


# --- End to end --------------------------------------------------------------


def test_e2e_accept_then_accept_again(client):
    store = client.app.state.store
    store.providers["p1"] = provider()
    store.jobs["j1"] = job()
    store.leases["l1"] = lease()

    first = client.post("/leases/l1/accept", json={"provider_id": "p1"})
    second = client.post("/leases/l1/accept", json={"provider_id": "p1"})

    assert first.status_code == 200
    # The contract's response shape. expires_at = Coordinator clock + configured 42 s.
    assert first.json() == {
        "lease_id": "l1",
        "lease_status": "ACTIVE",
        "expires_at": "2026-11-05T12:00:42Z",
        "job_id": "j1",
        "job_status": "STARTING",
        "attempt_count": 1,
    }
    # A repeated accept is refused and does not count a second attempt.
    assert second.status_code == 409
    assert second.json()["error"]["code"] == "INVALID_LEASE_STATE"
    assert store.jobs["j1"].attempt_count == 1
