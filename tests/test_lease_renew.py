"""POST /leases/{id}/renew, tested layer by layer: service -> route -> end to end."""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from coordinator.api import leases as leases_api
from coordinator.app import create_app
from coordinator.models.job import Job, JobStatus
from coordinator.models.lease import Lease, LeaseStatus, RenewRequest, RenewResponse
from coordinator.models.provider import Capabilities, Provider, ProviderStatus
from coordinator.services import lease_service
from coordinator.settings import load_settings
from coordinator.store import Store

NOW = datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc)
# The lease was accepted at NOW and runs until EXPIRES_AT. The renew arrives 3 s in.
EXPIRES_AT = NOW + timedelta(seconds=10)
RENEWED_AT = NOW + timedelta(seconds=3)
# In service tests, a renewed lease ends at most 60 s after the renew arrives.
MAX_REMAINING = 60
LIMIT = RENEWED_AT + timedelta(seconds=MAX_REMAINING)


def provider(status=ProviderStatus.ACTIVE, drain_deadline=None):
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


def job(status=JobStatus.RUNNING, current_lease_id="l1", attempt_count=1):
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


def run_renew(
    stored_provider=None,
    stored_job=None,
    stored_lease=None,
    lease_id="l1",
    provider_id="p1",
    extend_seconds=3,
    received_at=RENEWED_AT,
):
    """Send a renew for lease_id in a fresh Store holding p1, j1 and l1.

    By default the Job is RUNNING on l1, and the renew asks for 3 more seconds.
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
        request = RenewRequest(provider_id=provider_id, extend_seconds=extend_seconds)
        try:
            result = await lease_service.renew_lease(
                store, lease_id, request, received_at, MAX_REMAINING
            )
        except Exception as exc:
            result = exc
        return store, result

    return asyncio.run(scenario())


@pytest.fixture
def client():
    # Long loop intervals: the scheduler and monitor tick once at startup and then
    # stay out of the way. A limit unlike the YAML value shows the route really
    # passes the configured number.
    settings = load_settings().model_copy(
        update={
            "monitor_interval_seconds": 3600,
            "scheduler_interval_seconds": 3600,
            "lease_duration_seconds": 10,
            "max_lease_remaining_seconds": 15,
        }
    )
    with TestClient(create_app(clock=lambda: NOW, settings=settings)) as test_client:
        yield test_client


# --- Service layer -----------------------------------------------------------


def test_service_renew_adds_to_the_old_deadline():
    # Renewed 3 s in, by 3 s: the lease now ends 13 s in, not 6 s in.
    store, returned = run_renew(extend_seconds=3)

    new_expires_at = EXPIRES_AT + timedelta(seconds=3)
    assert returned == RenewResponse(
        lease_id="l1", lease_status=LeaseStatus.ACTIVE, expires_at=new_expires_at
    )
    # Only expires_at changes.
    assert store.leases["l1"] == lease(expires_at=new_expires_at)
    assert store.jobs["j1"] == job()
    assert store.providers["p1"] == provider()


@pytest.mark.parametrize(
    "stored_expires_at, extend_seconds, expected_expires_at",
    [
        (EXPIRES_AT, 3, EXPIRES_AT + timedelta(seconds=3)),
        (EXPIRES_AT, 53, LIMIT),
        (EXPIRES_AT, 365 * 24 * 3600, LIMIT),
        (LIMIT - timedelta(seconds=1), 3, LIMIT),
    ],
    ids=["under-the-limit", "exactly-the-limit", "a-whole-year", "time-already-piled-up"],
)
def test_service_renew_never_goes_past_the_limit(
    stored_expires_at, extend_seconds, expected_expires_at
):
    # The lease ends at most MAX_REMAINING seconds after the renew arrives.
    store, returned = run_renew(
        stored_lease=lease(expires_at=stored_expires_at), extend_seconds=extend_seconds
    )

    assert returned.expires_at == expected_expires_at
    assert store.leases["l1"].expires_at == expected_expires_at


@pytest.mark.parametrize(
    "provider_status, drain_deadline, extend_seconds, expected_expires_at",
    [
        (ProviderStatus.DRAINING, NOW + timedelta(seconds=20), 3, NOW + timedelta(seconds=13)),
        (ProviderStatus.DRAINING, NOW + timedelta(seconds=20), 30, NOW + timedelta(seconds=20)),
        (ProviderStatus.DRAINING, NOW + timedelta(seconds=5), 3, NOW + timedelta(seconds=5)),
        # A deadline left over from an old drain does not count any more.
        (ProviderStatus.ACTIVE, NOW + timedelta(seconds=5), 3, NOW + timedelta(seconds=13)),
    ],
    ids=["before-drain-deadline", "cut-at-drain-deadline", "moved-earlier", "not-draining"],
)
def test_service_draining_provider_must_finish_by_its_drain_deadline(
    provider_status, drain_deadline, extend_seconds, expected_expires_at
):
    stored_provider = provider(provider_status, drain_deadline=drain_deadline)

    store, returned = run_renew(stored_provider=stored_provider, extend_seconds=extend_seconds)

    assert returned.expires_at == expected_expires_at
    assert store.leases["l1"].expires_at == expected_expires_at
    assert store.providers["p1"] == stored_provider


@pytest.mark.parametrize(
    "received_at, expected",
    [
        (EXPIRES_AT - timedelta(microseconds=1), RenewResponse),
        (EXPIRES_AT, lease_service.LeaseExpired),
        (EXPIRES_AT + timedelta(seconds=1), lease_service.LeaseExpired),
    ],
    ids=["just-before-deadline", "at-deadline", "after-deadline"],
)
def test_service_deadline_moment_already_counts_as_expired(received_at, expected):
    # Same rule as started: once the deadline is reached, it is over.
    _, result = run_renew(received_at=received_at)

    assert isinstance(result, expected)


@pytest.mark.parametrize(
    "received_at, expected",
    [
        (NOW + timedelta(seconds=5) - timedelta(microseconds=1), RenewResponse),
        (NOW + timedelta(seconds=5), lease_service.DrainDeadlinePassed),
        (NOW + timedelta(seconds=6), lease_service.DrainDeadlinePassed),
    ],
    ids=["just-before-drain-deadline", "at-drain-deadline", "after-drain-deadline"],
)
def test_service_drain_deadline_moment_already_counts_as_over(received_at, expected):
    # The drain ends 5 s in, before the lease itself would end at 10 s.
    draining = provider(ProviderStatus.DRAINING, drain_deadline=NOW + timedelta(seconds=5))

    _, result = run_renew(stored_provider=draining, received_at=received_at)

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
                "stored_lease": lease(LeaseStatus.RELEASED),
                "stored_job": job(JobStatus.COMPLETED, current_lease_id=None),
            },
            lease_service.InvalidLeaseState,
        ),
        (
            {
                "stored_lease": lease(LeaseStatus.EXPIRED),
                "stored_job": job(JobStatus.QUEUED, current_lease_id=None),
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
        (
            {
                "stored_lease": lease(LeaseStatus.REJECTED, expires_at=None),
                "stored_job": job(JobStatus.QUEUED, current_lease_id=None, attempt_count=0),
            },
            lease_service.InvalidLeaseState,
        ),
        ({"received_at": EXPIRES_AT}, lease_service.LeaseExpired),
        ({"stored_provider": provider(ProviderStatus.DRAINED)}, lease_service.ProviderNotActive),
        ({"stored_provider": provider(ProviderStatus.STALE)}, lease_service.ProviderNotActive),
        ({"stored_job": job(current_lease_id="l2")}, lease_service.LeaseNotCurrent),
        ({"stored_job": job(JobStatus.STARTING)}, lease_service.InvalidJobState),
        (
            {"stored_provider": provider(ProviderStatus.DRAINING, drain_deadline=RENEWED_AT)},
            lease_service.DrainDeadlinePassed,
        ),
    ],
    ids=[
        "unknown-lease",
        "other-provider",
        "not-accepted-yet",
        "already-reported",
        "expired-by-monitor",
        "revoked",
        "rejected",
        "lease-expired",
        "provider-drained",
        "provider-stale",
        "job-moved-on",
        "not-started-yet",
        "drain-time-up",
    ],
)
def test_service_refused_renew_changes_nothing(case, expected):
    # The default Store contents, with this case's changes on top.
    seeds = {"stored_provider": provider(), "stored_job": job(), "stored_lease": lease(), **case}

    store, raised = run_renew(**seeds)

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
        (lease_service.InvalidLeaseState("l1", "RELEASED", "ACTIVE"), 409, "INVALID_LEASE_STATE"),
        (lease_service.LeaseExpired("l1", EXPIRES_AT), 409, "LEASE_EXPIRED"),
        (lease_service.ProviderNotActive("p1", "STALE"), 409, "PROVIDER_NOT_ACTIVE"),
        (lease_service.LeaseNotCurrent("l1", "j1"), 409, "LEASE_NOT_CURRENT"),
        (lease_service.InvalidJobState("j1", "STARTING", "RUNNING"), 409, "INVALID_JOB_STATE"),
        (lease_service.DrainDeadlinePassed("p1", NOW), 409, "DRAIN_DEADLINE_PASSED"),
    ],
    ids=[
        "not-found",
        "other-provider",
        "not-active",
        "lease-expired",
        "provider-not-active",
        "not-current",
        "job-not-running",
        "drain-time-up",
    ],
)
def test_route_translates_service_errors(client, monkeypatch, exception, status_code, error_code):
    async def fake_renew(*args):
        raise exception

    # The route imports renew_lease by name, so replace it where the route looks it up.
    monkeypatch.setattr(leases_api, "renew_lease", fake_renew)

    response = client.post("/leases/l1/renew", json={"provider_id": "p1", "extend_seconds": 3})

    assert response.status_code == status_code
    assert response.json()["error"]["code"] == error_code


@pytest.mark.parametrize(
    "body",
    [
        {"extend_seconds": 3},
        {"provider_id": "p1"},
        {"provider_id": "p1", "extend_seconds": 0},
        {"provider_id": "p1", "extend_seconds": -1},
        {"provider_id": "p1", "extend_seconds": "soon"},
        {"provider_id": "p1", "extend_seconds": 3, "outcome": "SUCCESS"},
    ],
    ids=[
        "no-provider-id",
        "no-extend-seconds",
        "zero-seconds",
        "negative-seconds",
        "not-a-number",
        "unknown-field",
    ],
)
def test_route_rejects_bad_body_before_the_service(client, monkeypatch, body):
    async def fake_renew(*args):
        pytest.fail("Invalid input must not reach the service")

    monkeypatch.setattr(leases_api, "renew_lease", fake_renew)

    response = client.post("/leases/l1/renew", json=body)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


# --- End to end --------------------------------------------------------------


def test_e2e_renew_needs_started_and_stays_within_the_limit(client):
    store = client.app.state.store
    store.providers["p1"] = provider()
    store.jobs["j1"] = job(JobStatus.QUEUED, attempt_count=0)
    store.leases["l1"] = lease(LeaseStatus.OFFERED, expires_at=None)

    accepted = client.post("/leases/l1/accept", json={"provider_id": "p1"})
    renew_3_seconds = {"provider_id": "p1", "extend_seconds": 3}
    too_early = client.post("/leases/l1/renew", json=renew_3_seconds)
    started = client.post("/leases/l1/started", json={"provider_id": "p1"})
    renewed = client.post("/leases/l1/renew", json=renew_3_seconds)
    renewed_a_lot = client.post(
        "/leases/l1/renew", json={"provider_id": "p1", "extend_seconds": 100}
    )

    assert accepted.status_code == 200
    # No started yet, so no extra time.
    assert too_early.status_code == 409
    assert too_early.json()["error"]["code"] == "INVALID_JOB_STATE"
    assert started.status_code == 200
    # The contract's response shape. 3 s are added to the old end at 12:00:10.
    assert renewed.status_code == 200
    assert renewed.json() == {
        "lease_id": "l1",
        "lease_status": "ACTIVE",
        "expires_at": "2026-11-05T12:00:13Z",
    }
    # The fixture's limit: at most 15 s after the renew, which arrives at 12:00:00.
    assert renewed_a_lot.json()["expires_at"] == "2026-11-05T12:00:15Z"
    assert client.get("/leases/l1").json()["expires_at"] == "2026-11-05T12:00:15Z"
