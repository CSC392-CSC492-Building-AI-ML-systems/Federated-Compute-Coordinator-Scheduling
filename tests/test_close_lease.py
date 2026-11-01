"""close_lease and the retry check, tested layer by layer: policy -> service helper.

close_lease has no route. Its callers (reject, report, drain, monitor checks) call it
while holding the Store lock, so these tests hold the lock too.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from coordinator.models.job import Job, JobStatus
from coordinator.models.lease import (
    FailureReason,
    InternalReason,
    Lease,
    LeaseOutcome,
    LeaseStatus,
    RejectReason,
)
from coordinator.policies.retry import should_retry
from coordinator.services import lease_service
from coordinator.store import Store

NOW = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)
# Later than NOW, so a timestamp set by close_lease is easy to tell apart.
CLOSED_AT = NOW + timedelta(seconds=30)
MAX_ATTEMPTS = 3


def job(status, current_lease_id="l1", attempt_count=0):
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


def lease(status, lease_id="l1"):
    return Lease(
        lease_id=lease_id,
        job_id="j1",
        provider_id="p1",
        status=status,
        offer_deadline=NOW,
        created_at=NOW,
    )


def run_close(stored_lease, stored_job, status, reason, outcome=None):
    """Close stored_lease in a fresh Store. Returns (store, None or the raised exception)."""

    async def scenario():
        # Store is created inside the event loop (asyncio.Lock on Python 3.9).
        store = Store()
        store.leases[stored_lease.lease_id] = stored_lease
        store.jobs[stored_job.job_id] = stored_job
        raised = None
        async with store.lock:
            try:
                lease_service.close_lease(
                    store,
                    stored_lease.lease_id,
                    status=status,
                    reason=reason,
                    now=CLOSED_AT,
                    max_attempts=MAX_ATTEMPTS,
                    outcome=outcome,
                )
            except Exception as exc:
                raised = exc
        return store, raised

    return asyncio.run(scenario())


# --- Policy ------------------------------------------------------------------


@pytest.mark.parametrize(
    "attempt_count, expected",
    [(1, True), (2, True), (3, False), (4, False)],
    ids=["first-attempt", "last-attempt-left", "attempts-used-up", "over-the-limit"],
)
def test_policy_retries_only_while_attempts_remain(attempt_count, expected):
    # accept already counted the failed attempt, so 3 of 3 means none are left.
    failed_job = job(JobStatus.RUNNING, attempt_count=attempt_count)

    assert should_retry(failed_job, FailureReason.DISK_FULL.value, MAX_ATTEMPTS) is expected


# --- Service layer -----------------------------------------------------------


@pytest.mark.parametrize(
    "status, reason",
    [
        (LeaseStatus.REJECTED, RejectReason.LOCAL_BUSY.value),
        (LeaseStatus.EXPIRED, InternalReason.OFFER_TIMEOUT.value),
        (LeaseStatus.REVOKED, InternalReason.DRAIN_STARTED.value),
    ],
    ids=["rejected", "offer-expired", "offer-revoked"],
)
def test_ended_offer_keeps_job_queued_without_using_an_attempt(status, reason):
    store, raised = run_close(lease(LeaseStatus.OFFERED), job(JobStatus.QUEUED), status, reason)

    assert raised is None
    closed = store.leases["l1"]
    assert closed.status is status
    assert closed.reason == reason
    assert closed.released_at is None
    # Never accepted, so no attempt was used (invariant 7); the scheduler may offer
    # the Job again once current_lease_id is cleared.
    assert store.jobs["j1"] == job(JobStatus.QUEUED, current_lease_id=None)


def test_success_report_completes_job():
    store, raised = run_close(
        lease(LeaseStatus.ACTIVE),
        job(JobStatus.RUNNING, attempt_count=1),
        LeaseStatus.RELEASED,
        None,
        outcome=LeaseOutcome.SUCCESS,
    )

    assert raised is None
    closed = store.leases["l1"]
    assert closed.status is LeaseStatus.RELEASED
    assert closed.outcome is LeaseOutcome.SUCCESS
    assert closed.released_at == CLOSED_AT
    assert store.jobs["j1"] == job(JobStatus.COMPLETED, current_lease_id=None, attempt_count=1)


@pytest.mark.parametrize(
    "status, reason, outcome",
    [
        (LeaseStatus.RELEASED, FailureReason.DISK_FULL.value, LeaseOutcome.FAILURE),
        (LeaseStatus.EXPIRED, InternalReason.EXECUTION_TIMEOUT.value, None),
        (LeaseStatus.REVOKED, InternalReason.PROVIDER_STALE.value, None),
    ],
    ids=["failure-report", "execution-timeout", "provider-stale"],
)
def test_failed_attempt_requeues_job_while_attempts_remain(status, reason, outcome):
    store, raised = run_close(
        lease(LeaseStatus.ACTIVE),
        job(JobStatus.RUNNING, attempt_count=1),
        status,
        reason,
        outcome=outcome,
    )

    assert raised is None
    assert store.leases["l1"].status is status
    stored_job = store.jobs["j1"]
    assert stored_job.status is JobStatus.QUEUED
    assert stored_job.current_lease_id is None
    assert stored_job.last_failure_reason == reason
    # Going back to the queue does not count as an attempt; the next accept does.
    assert stored_job.attempt_count == 1


def test_failed_last_attempt_fails_job():
    # The case expire_heartbeats misses today: a Provider keeps going stale and the
    # Job used to be requeued forever.
    store, raised = run_close(
        lease(LeaseStatus.ACTIVE),
        job(JobStatus.RUNNING, attempt_count=MAX_ATTEMPTS),
        LeaseStatus.REVOKED,
        InternalReason.PROVIDER_STALE.value,
    )

    assert raised is None
    stored_job = store.jobs["j1"]
    assert stored_job.status is JobStatus.FAILED
    assert stored_job.current_lease_id is None
    assert stored_job.last_failure_reason == "PROVIDER_STALE"


def test_old_lease_closes_without_touching_job():
    # The Job has already moved on to l2, so l1 ending must not change it
    # (invariants 5 and 10).
    current_job = job(JobStatus.RUNNING, current_lease_id="l2", attempt_count=2)

    store, raised = run_close(
        lease(LeaseStatus.ACTIVE),
        current_job,
        LeaseStatus.REVOKED,
        InternalReason.PROVIDER_STALE.value,
    )

    assert raised is None
    assert store.leases["l1"].status is LeaseStatus.REVOKED
    assert store.jobs["j1"] == job(JobStatus.RUNNING, current_lease_id="l2", attempt_count=2)


@pytest.mark.parametrize(
    "lease_status, job_status, status, outcome",
    [
        (LeaseStatus.REVOKED, JobStatus.QUEUED, LeaseStatus.REVOKED, None),
        (LeaseStatus.OFFERED, JobStatus.QUEUED, LeaseStatus.RELEASED, LeaseOutcome.SUCCESS),
        (LeaseStatus.ACTIVE, JobStatus.RUNNING, LeaseStatus.REJECTED, None),
        (LeaseStatus.ACTIVE, JobStatus.RUNNING, LeaseStatus.REVOKED, LeaseOutcome.SUCCESS),
        (LeaseStatus.ACTIVE, JobStatus.RUNNING, LeaseStatus.RELEASED, None),
    ],
    ids=[
        "already-ended",
        "offer-cannot-be-released",
        "accepted-cannot-be-rejected",
        "outcome-without-release",
        "release-without-outcome",
    ],
)
def test_caller_bug_raises_and_changes_nothing(lease_status, job_status, status, outcome):
    store, raised = run_close(lease(lease_status), job(job_status), status, None, outcome=outcome)

    assert isinstance(raised, ValueError)
    # Checks run before any change, because the Store has no rollback.
    assert store.leases["l1"] == lease(lease_status)
    assert store.jobs["j1"] == job(job_status)
