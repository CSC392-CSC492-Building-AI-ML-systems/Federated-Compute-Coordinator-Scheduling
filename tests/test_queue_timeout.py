"""Queue timeout, tested layer by layer: service -> monitor -> end to end."""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.models.job import Job, JobStatus
from coordinator.models.lease import Lease, LeaseStatus
from coordinator.monitor import run_monitor_tick
from coordinator.services import job_service
from coordinator.settings import load_settings
from coordinator.store import Store

NOW = datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc)
# A limit unlike the YAML value shows the monitor really passes the configured number.
QUEUE_TIMEOUT = 20
# A Job submitted at NOW has waited exactly the limit at this moment.
DEADLINE = NOW + timedelta(seconds=QUEUE_TIMEOUT)
SETTINGS = load_settings().model_copy(
    update={
        "queue_timeout_seconds": QUEUE_TIMEOUT,
        # The app's own loops tick once at startup and then stay out of the way.
        "monitor_interval_seconds": 3600,
        "scheduler_interval_seconds": 3600,
    }
)


def job(status=JobStatus.QUEUED, current_lease_id=None, attempt_count=0):
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


def timed_out(stored_job):
    """What stored_job looks like after the queue timeout. Nothing else changes."""
    return stored_job.model_copy(
        update={"status": JobStatus.FAILED, "last_failure_reason": "QUEUE_TIMEOUT"}
    )


def open_offer():
    """j1 offered to p1 after the limit passed. p1 has not answered yet."""
    return Lease(
        lease_id="l1",
        job_id="j1",
        provider_id="p1",
        status=LeaseStatus.OFFERED,
        offer_deadline=DEADLINE + timedelta(seconds=5),
        created_at=DEADLINE,
    )


def run_check(stored_job, now, stored_lease=None):
    """Run one queue timeout check on a fresh Store holding j1, and l1 if given.

    The Store gets copies, so the objects passed in stay as they were and a test can
    compare against them. Returns the store.
    """

    async def scenario():
        # Store is created inside the event loop (asyncio.Lock on Python 3.9).
        store = Store()
        store.jobs["j1"] = stored_job.model_copy(deep=True)
        if stored_lease is not None:
            store.leases["l1"] = stored_lease.model_copy(deep=True)
        await job_service.expire_queued_jobs(store, now, QUEUE_TIMEOUT)
        return store

    return asyncio.run(scenario())


# --- Service layer -----------------------------------------------------------


@pytest.mark.parametrize(
    "offset, fails",
    [(-0.001, False), (0, True), (0.001, True)],
    ids=["just-before", "exactly-at", "just-after"],
)
def test_service_queued_job_fails_once_it_waited_too_long(offset, fails):
    store = run_check(job(), DEADLINE + timedelta(seconds=offset))

    # Waiting never uses an attempt, so attempt_count stays 0 either way.
    assert store.jobs["j1"] == (timed_out(job()) if fails else job())


@pytest.mark.parametrize(
    "status",
    [status for status in JobStatus if status is not JobStatus.QUEUED],
    ids=lambda status: status.value,
)
def test_service_job_that_left_the_queue_is_untouched(status):
    # Long past the limit, but the Job already started or ended.
    store = run_check(job(status), DEADLINE + timedelta(hours=1))

    assert store.jobs["j1"] == job(status)


def test_service_job_with_an_open_offer_waits_for_the_answer():
    # Past the limit, but p1 may still accept, so the Job is left alone for now.
    waiting = job(current_lease_id="l1")
    store = run_check(waiting, DEADLINE + timedelta(seconds=1), stored_lease=open_offer())

    assert store.jobs["j1"] == waiting
    assert store.leases["l1"] == open_offer()


def test_service_job_back_from_a_failed_run_keeps_its_submission_time():
    # It ran once, failed and came back to the queue. Time still counts from submission.
    returned = job(attempt_count=1).model_copy(update={"last_failure_reason": "DISK_FULL"})
    store = run_check(returned, DEADLINE)

    assert store.jobs["j1"] == timed_out(returned)


def test_service_skipping_one_job_still_checks_the_next():
    async def scenario():
        store = Store()
        # j1 was only just submitted, so it is skipped. j2 waited the full limit.
        store.jobs["j1"] = job().model_copy(update={"created_at": DEADLINE})
        store.jobs["j2"] = job().model_copy(update={"job_id": "j2"})
        await job_service.expire_queued_jobs(store, DEADLINE, QUEUE_TIMEOUT)
        return store

    store = asyncio.run(scenario())

    assert store.jobs["j1"].status is JobStatus.QUEUED
    assert store.jobs["j2"].status is JobStatus.FAILED


def test_service_check_waits_for_the_store_lock():
    async def scenario():
        store = Store()
        store.jobs["j1"] = job()
        async with store.lock:
            task = asyncio.create_task(
                job_service.expire_queued_jobs(store, DEADLINE, QUEUE_TIMEOUT)
            )
            await asyncio.sleep(0)
            # Another request holds the lock, so the check has not changed anything yet.
            assert not task.done()
            assert store.jobs["j1"] == job()
        await asyncio.wait_for(task, timeout=1)
        return store

    store = asyncio.run(scenario())

    assert store.jobs["j1"] == timed_out(job())


# --- Monitor -----------------------------------------------------------------


def test_monitor_offer_that_runs_out_lets_the_queue_timeout_fail_the_job():
    # The offer runs out after the limit passed.
    # In the same tick, the offer ends first, then the Job times out.
    offer = open_offer()

    async def scenario():
        store = Store()
        store.jobs["j1"] = job(current_lease_id="l1")
        store.leases["l1"] = offer.model_copy(deep=True)
        await run_monitor_tick(store, offer.offer_deadline, SETTINGS)
        return store

    store = asyncio.run(scenario())

    assert store.leases["l1"].status is LeaseStatus.EXPIRED
    assert store.leases["l1"].reason == "OFFER_TIMEOUT"
    stored_job = store.jobs["j1"]
    assert stored_job.status is JobStatus.FAILED
    assert stored_job.last_failure_reason == "QUEUE_TIMEOUT"
    assert stored_job.current_lease_id is None
    assert stored_job.attempt_count == 0


# --- End to end --------------------------------------------------------------


def test_e2e_job_no_provider_can_run_fails_after_the_limit():
    # No Provider is registered, so nobody can ever take this Job.
    body = {"required_vram_mb": 81920, "required_runtime": "cuda12", "tier": "standard"}
    with TestClient(create_app(clock=lambda: NOW, settings=SETTINGS)) as client:
        job_id = client.post("/jobs", json=body).json()["job_id"]
        store = client.app.state.store

        # Run the monitor on the app's own event loop, as its background loop would.
        client.portal.call(run_monitor_tick, store, DEADLINE - timedelta(seconds=1), SETTINGS)
        before = client.get(f"/jobs/{job_id}").json()
        client.portal.call(run_monitor_tick, store, DEADLINE, SETTINGS)
        after = client.get(f"/jobs/{job_id}").json()

    assert before["status"] == "QUEUED"
    assert after["status"] == "FAILED"
    assert after["last_failure_reason"] == "QUEUE_TIMEOUT"
    assert after["attempt_count"] == 0
