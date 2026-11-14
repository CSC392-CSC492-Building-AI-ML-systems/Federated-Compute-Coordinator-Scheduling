"""POST /leases/{id}/report, tested layer by layer: service -> route -> end to end."""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from coordinator.api import leases as leases_api
from coordinator.app import create_app
from coordinator.models.job import Job, JobStatus
from coordinator.models.lease import (
    FailureReason,
    Lease,
    LeaseOutcome,
    LeaseStatus,
    ReportRequest,
    ReportResponse,
)
from coordinator.models.provider import Capabilities, Provider, ProviderStatus
from coordinator.services import lease_service
from coordinator.settings import load_settings
from coordinator.store import Store

NOW = datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc)
# The lease was accepted at NOW and runs until EXPIRES_AT. The report arrives 5 s in.
EXPIRES_AT = NOW + timedelta(seconds=10)
REPORTED_AT = NOW + timedelta(seconds=5)
MAX_ATTEMPTS = 3


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


def run_report(
    stored_provider=None,
    stored_job=None,
    stored_lease=None,
    lease_id="l1",
    provider_id="p1",
    outcome=LeaseOutcome.SUCCESS,
    reason=None,
    received_at=REPORTED_AT,
):
    """Send a report for lease_id in a fresh Store holding p1, j1 and l1.

    By default the Job is RUNNING on l1, and the report says SUCCESS.
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
        request = ReportRequest(provider_id=provider_id, outcome=outcome, reason=reason)
        try:
            result = await lease_service.report_lease(
                store, lease_id, request, received_at, MAX_ATTEMPTS
            )
        except Exception as exc:
            result = exc
        return store, result

    return asyncio.run(scenario())


@pytest.fixture
def client():
    # Long loop intervals: the scheduler and monitor tick once at startup and then
    # stay out of the way. A max_attempts unlike the YAML value shows the route
    # really passes the configured number.
    settings = load_settings().model_copy(
        update={
            "monitor_interval_seconds": 3600,
            "scheduler_interval_seconds": 3600,
            "max_attempts": 1,
        }
    )
    with TestClient(create_app(clock=lambda: NOW, settings=settings)) as test_client:
        yield test_client


# --- Service layer -----------------------------------------------------------


@pytest.mark.parametrize(
    "provider_status",
    [ProviderStatus.ACTIVE, ProviderStatus.DRAINING],
    ids=["provider-active", "provider-draining"],
)
def test_service_success_completes_job(provider_status):
    # A draining Provider is finishing its work, so its report is taken.
    store, returned = run_report(stored_provider=provider(provider_status))

    assert returned == ReportResponse(
        lease_id="l1",
        lease_status=LeaseStatus.RELEASED,
        outcome=LeaseOutcome.SUCCESS,
        job_id="j1",
        job_status=JobStatus.COMPLETED,
    )
    stored_lease = store.leases["l1"]
    assert stored_lease.status is LeaseStatus.RELEASED
    assert stored_lease.outcome is LeaseOutcome.SUCCESS
    assert stored_lease.reason is None
    assert stored_lease.released_at == REPORTED_AT
    # The attempt was already counted at accept.
    assert store.jobs["j1"] == job(JobStatus.COMPLETED, current_lease_id=None)
    assert store.providers["p1"] == provider(provider_status)


@pytest.mark.parametrize(
    "job_status",
    [JobStatus.STARTING, JobStatus.RUNNING],
    ids=["failed-while-starting", "failed-while-running"],
)
@pytest.mark.parametrize(
    "attempt_count, expected_job_status",
    [(1, JobStatus.QUEUED), (MAX_ATTEMPTS, JobStatus.FAILED)],
    ids=["attempts-left", "no-attempts-left"],
)
def test_service_failure_runs_retry_check(job_status, attempt_count, expected_job_status):
    store, returned = run_report(
        stored_job=job(job_status, attempt_count=attempt_count),
        outcome=LeaseOutcome.FAILURE,
        reason=FailureReason.DISK_FULL,
    )

    assert returned == ReportResponse(
        lease_id="l1",
        lease_status=LeaseStatus.RELEASED,
        outcome=LeaseOutcome.FAILURE,
        job_id="j1",
        job_status=expected_job_status,
    )
    stored_lease = store.leases["l1"]
    assert stored_lease.status is LeaseStatus.RELEASED
    assert stored_lease.outcome is LeaseOutcome.FAILURE
    assert stored_lease.reason == "DISK_FULL"
    stored_job = store.jobs["j1"]
    assert stored_job.status is expected_job_status
    assert stored_job.current_lease_id is None
    # A FAILED Job can explain itself.
    assert stored_job.last_failure_reason == "DISK_FULL"
    # A report never counts an attempt. Only accept does.
    assert stored_job.attempt_count == attempt_count


def test_service_failure_without_a_reason_is_taken():
    # The contract makes the reason optional.
    store, returned = run_report(outcome=LeaseOutcome.FAILURE)

    assert returned.job_status is JobStatus.QUEUED
    assert store.leases["l1"].reason is None
    assert store.jobs["j1"].last_failure_reason is None


@pytest.mark.parametrize("reason", list(FailureReason), ids=lambda reason: reason.value)
def test_service_only_capability_mismatch_stops_offers_to_this_provider(reason):
    store, returned = run_report(outcome=LeaseOutcome.FAILURE, reason=reason)

    # The Job still runs again. After a CAPABILITY_MISMATCH, just not on p1.
    assert returned.job_status is JobStatus.QUEUED
    expected = {"p1"} if reason is FailureReason.CAPABILITY_MISMATCH else set()
    assert store.jobs["j1"].rejected_provider_ids == expected
    # Only the Job remembers it. The Provider record is unchanged.
    assert store.providers["p1"] == provider()


@pytest.mark.parametrize(
    "received_at, expected",
    [
        (EXPIRES_AT - timedelta(microseconds=1), ReportResponse),
        (EXPIRES_AT, lease_service.LeaseExpired),
        (EXPIRES_AT + timedelta(seconds=1), lease_service.LeaseExpired),
    ],
    ids=["just-before-deadline", "at-deadline", "after-deadline"],
)
def test_service_deadline_moment_already_counts_as_expired(received_at, expected):
    # Same rule as started: once the deadline is reached, it is over.
    _, result = run_report(received_at=received_at)

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
        (
            {"stored_job": job(JobStatus.QUEUED), "outcome": LeaseOutcome.FAILURE},
            lease_service.InvalidJobState,
        ),
        ({"stored_job": job(JobStatus.STARTING)}, lease_service.InvalidJobState),
        # A refused report must not stop offers to this Provider either.
        (
            {
                "outcome": LeaseOutcome.FAILURE,
                "reason": FailureReason.CAPABILITY_MISMATCH,
                "received_at": EXPIRES_AT,
            },
            lease_service.LeaseExpired,
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
        "job-queued",
        "success-before-started",
        "late-capability-mismatch",
    ],
)
def test_service_refused_report_changes_nothing(case, expected):
    # The default Store contents, with this case's changes on top.
    seeds = {"stored_provider": provider(), "stored_job": job(), "stored_lease": lease(), **case}

    store, raised = run_report(**seeds)

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
    ],
    ids=[
        "not-found",
        "other-provider",
        "not-active",
        "lease-expired",
        "provider-not-active",
        "not-current",
        "job-not-running",
    ],
)
def test_route_translates_service_errors(client, monkeypatch, exception, status_code, error_code):
    async def fake_report(*args):
        raise exception

    # The route imports report_lease by name, so replace it where the route looks it up.
    monkeypatch.setattr(leases_api, "report_lease", fake_report)

    response = client.post("/leases/l1/report", json={"provider_id": "p1", "outcome": "SUCCESS"})

    assert response.status_code == status_code
    assert response.json()["error"]["code"] == error_code


@pytest.mark.parametrize(
    "body",
    [
        {"outcome": "SUCCESS"},
        {"provider_id": "p1"},
        {"provider_id": "p1", "outcome": "PARTIAL"},
        {"provider_id": "p1", "outcome": "FAILURE", "reason": "OFFER_TIMEOUT"},
        {"provider_id": "p1", "outcome": "SUCCESS", "reason": "DISK_FULL"},
        {"provider_id": "p1", "outcome": "SUCCESS", "extend_seconds": 5},
    ],
    ids=[
        "no-provider-id",
        "no-outcome",
        "unknown-outcome",
        "internal-reason",
        "success-with-reason",
        "unknown-field",
    ],
)
def test_route_rejects_bad_body_before_the_service(client, monkeypatch, body):
    async def fake_report(*args):
        pytest.fail("Invalid input must not reach the service")

    monkeypatch.setattr(leases_api, "report_lease", fake_report)

    response = client.post("/leases/l1/report", json=body)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


# --- End to end --------------------------------------------------------------


def seed_open_offer(client):
    store = client.app.state.store
    store.providers["p1"] = provider()
    store.jobs["j1"] = job(JobStatus.QUEUED, attempt_count=0)
    store.leases["l1"] = lease(LeaseStatus.OFFERED, expires_at=None)


def test_e2e_accept_started_report_then_report_again(client):
    seed_open_offer(client)

    accepted = client.post("/leases/l1/accept", json={"provider_id": "p1"})
    started = client.post("/leases/l1/started", json={"provider_id": "p1"})
    success = {"provider_id": "p1", "outcome": "SUCCESS"}
    reported = client.post("/leases/l1/report", json=success)
    reported_again = client.post("/leases/l1/report", json=success)

    assert accepted.status_code == 200
    assert started.status_code == 200
    assert reported.status_code == 200
    # The contract's response shape.
    assert reported.json() == {
        "lease_id": "l1",
        "lease_status": "RELEASED",
        "outcome": "SUCCESS",
        "job_id": "j1",
        "job_status": "COMPLETED",
    }
    stored_lease = client.get("/leases/l1").json()
    assert stored_lease["outcome"] == "SUCCESS"
    assert stored_lease["released_at"] == "2026-11-05T12:00:00Z"
    # The lease has ended, so a second report is refused and the Job stays COMPLETED.
    assert reported_again.status_code == 409
    assert reported_again.json()["error"]["code"] == "INVALID_LEASE_STATE"
    assert client.get("/jobs/j1").json()["status"] == "COMPLETED"


def test_e2e_startup_failure_uses_configured_max_attempts(client):
    seed_open_offer(client)

    accepted = client.post("/leases/l1/accept", json={"provider_id": "p1"})
    # No started: the Job fails while it is starting.
    failure = {"provider_id": "p1", "outcome": "FAILURE", "reason": "RUNTIME_ERROR"}
    reported = client.post("/leases/l1/report", json=failure)

    assert accepted.status_code == 200
    assert reported.status_code == 200
    # The fixture allows 1 attempt and accept used it, so the Job fails for good.
    assert reported.json()["job_status"] == "FAILED"
    assert client.get("/jobs/j1").json()["last_failure_reason"] == "RUNTIME_ERROR"
