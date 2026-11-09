"""Lease deadlines, offer exclusions, and their integration with the monitor."""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from coordinator.models.job import Job, JobStatus
from coordinator.models.lease import TERMINAL_LEASE_STATUSES, Lease, LeaseStatus
from coordinator.models.provider import Capabilities, Provider, ProviderStatus
from coordinator.monitor import run_monitor_tick
from coordinator.scheduler import run_matching
from coordinator.services import lease_service
from coordinator.settings import load_settings
from coordinator.store import Store

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=timezone.utc)
SETTINGS = load_settings().model_copy(
    update={"max_attempts": 2, "offer_timeouts_before_exclude": 2}
)


@pytest.fixture
def store():
    state = Store()
    state.providers["p1"] = Provider(
        provider_id="p1",
        status=ProviderStatus.ACTIVE,
        capabilities=Capabilities(
            gpu_model="A100", vram_mb=40960, driver_version="550", runtimes=["cuda12"]
        ),
        accepted_tiers=["standard"],
        registered_at=NOW,
        last_heartbeat_at=NOW,
    )
    state.jobs["j1"] = Job(
        job_id="j1",
        status=JobStatus.QUEUED,
        required_vram_mb=8192,
        gpu_models=[],
        required_runtime="cuda12",
        tier="standard",
        current_lease_id="l1",
        created_at=NOW,
    )
    state.leases["l1"] = Lease(
        lease_id="l1",
        job_id="j1",
        provider_id="p1",
        status=LeaseStatus.OFFERED,
        offer_deadline=NOW,
        created_at=NOW,
    )
    return state


@pytest.mark.parametrize("offset, expired", [(-0.001, False), (0, True), (0.001, True)])
def test_offer_deadline_boundary(store, offset, expired):
    now = NOW + timedelta(seconds=offset)
    asyncio.run(run_monitor_tick(store, now, SETTINGS))

    lease, job = store.leases["l1"], store.jobs["j1"]
    assert lease.status == (LeaseStatus.EXPIRED if expired else LeaseStatus.OFFERED)
    assert lease.reason == ("OFFER_TIMEOUT" if expired else None)
    assert job.current_lease_id == (None if expired else "l1")
    assert job.status == JobStatus.QUEUED
    assert job.attempt_count == 0
    assert job.last_failure_reason is None
    assert job.offer_timeout_counts == ({"p1": 1} if expired else {})


def test_repeated_offer_timeouts_exclude_only_that_job_provider_pair(store):
    async def scenario():
        # Another Job's timeout count and exclusions are independent.
        other_job = store.jobs["j1"].model_copy(
            deep=True, update={"job_id": "j2", "current_lease_id": None}
        )
        store.jobs["j2"] = other_job
        await run_monitor_tick(store, NOW, SETTINGS)
        assert store.jobs["j1"].offer_timeout_counts == {"p1": 1}
        assert store.jobs["j1"].rejected_provider_ids == set()

        # The first timeout still allows an offer to the same Provider.
        await run_matching(NOW, store, SETTINGS)
        second_id = store.jobs["j1"].current_lease_id
        assert second_id is not None
        deadline = store.leases[second_id].offer_deadline
        store.providers["p1"].last_heartbeat_at = deadline
        await run_monitor_tick(store, deadline, SETTINGS)

        assert store.jobs["j1"].offer_timeout_counts == {"p1": 2}
        assert store.jobs["j1"].rejected_provider_ids == {"p1"}
        assert other_job.offer_timeout_counts == {}
        assert other_job.rejected_provider_ids == set()

        store.providers["p2"] = store.providers["p1"].model_copy(
            deep=True, update={"provider_id": "p2"}
        )
        await run_matching(deadline, store, SETTINGS)
        current = store.leases[store.jobs["j1"].current_lease_id]
        assert current.provider_id == "p2"
        assert store.leases[other_job.current_lease_id].provider_id == "p1"
        # A different Provider's first timeout does not inherit p1's count.
        await run_monitor_tick(store, current.offer_deadline, SETTINGS)
        assert store.jobs["j1"].offer_timeout_counts == {"p1": 2, "p2": 1}
        assert store.jobs["j1"].rejected_provider_ids == {"p1"}
        assert store.jobs["j1"].attempt_count == 0

    asyncio.run(scenario())


def test_offer_exclusion_uses_configured_threshold(store):
    settings = SETTINGS.model_copy(update={"offer_timeouts_before_exclude": 1})
    asyncio.run(run_monitor_tick(store, NOW, settings))
    assert store.jobs["j1"].rejected_provider_ids == {"p1"}


@pytest.mark.parametrize("offset, expired", [(-0.001, False), (0, True), (0.001, True)])
@pytest.mark.parametrize("job_status", [JobStatus.STARTING, JobStatus.RUNNING])
@pytest.mark.parametrize("attempts", [1, 2])
def test_execution_deadline_and_retry_budget(store, offset, expired, job_status, attempts):
    lease, job = store.leases["l1"], store.jobs["j1"]
    lease.status = LeaseStatus.ACTIVE
    lease.expires_at = NOW
    job.status = job_status
    job.attempt_count = attempts

    asyncio.run(run_monitor_tick(store, NOW + timedelta(seconds=offset), SETTINGS))

    assert lease.status == (LeaseStatus.EXPIRED if expired else LeaseStatus.ACTIVE)
    assert lease.reason == ("EXECUTION_TIMEOUT" if expired else None)
    expected = JobStatus.QUEUED if attempts < SETTINGS.max_attempts else JobStatus.FAILED
    assert job.status == (expected if expired else job_status)
    assert job.current_lease_id == (None if expired else "l1")
    assert job.last_failure_reason == ("EXECUTION_TIMEOUT" if expired else None)
    assert job.attempt_count == attempts
    assert job.offer_timeout_counts == {}
    assert job.rejected_provider_ids == set()
    assert lease.released_at is None


def test_renewed_or_missing_execution_deadline_is_not_expired(store):
    async def scenario():
        lease = store.leases["l1"]
        lease.status = LeaseStatus.ACTIVE
        store.jobs["j1"].status = JobStatus.RUNNING
        store.jobs["j1"].attempt_count = 1
        for deadline in [None, NOW + timedelta(seconds=10)]:
            lease.expires_at = deadline
            before = lease.model_dump(), store.jobs["j1"].model_dump()
            await lease_service.expire_execution_leases(store, NOW, SETTINGS.max_attempts)
            assert (lease.model_dump(), store.jobs["j1"].model_dump()) == before

    asyncio.run(scenario())


@pytest.mark.parametrize("status", list(TERMINAL_LEASE_STATUSES))
def test_timeouts_preserve_terminal_leases(store, status):
    store.leases["l1"].status = status
    store.leases["l1"].expires_at = NOW
    before = store.leases["l1"].model_dump(), store.jobs["j1"].model_dump()

    asyncio.run(run_monitor_tick(store, NOW, SETTINGS))

    assert (store.leases["l1"].model_dump(), store.jobs["j1"].model_dump()) == before


@pytest.mark.parametrize("status", [LeaseStatus.OFFERED, LeaseStatus.ACTIVE])
def test_expired_old_lease_does_not_change_current_job(store, status):
    store.leases["l1"].status = status
    store.leases["l1"].expires_at = NOW
    job = store.jobs["j1"]
    job.status = JobStatus.RUNNING
    job.attempt_count = 1
    job.current_lease_id = "new-lease"
    before = job.model_dump()

    asyncio.run(run_monitor_tick(store, NOW, SETTINGS))

    assert store.leases["l1"].status == LeaseStatus.EXPIRED
    assert job.model_dump() == before


@pytest.mark.parametrize("status", [LeaseStatus.OFFERED, LeaseStatus.ACTIVE])
def test_another_tick_does_not_close_or_count_a_lease_twice(store, status):
    async def scenario():
        lease = store.leases["l1"]
        lease.status = status
        lease.expires_at = NOW
        if status == LeaseStatus.ACTIVE:
            store.jobs["j1"].status = JobStatus.RUNNING
            store.jobs["j1"].attempt_count = 1
        await run_monitor_tick(store, NOW, SETTINGS)
        before = lease.model_dump(), store.jobs["j1"].model_dump()
        await run_monitor_tick(store, NOW, SETTINGS)
        assert (lease.model_dump(), store.jobs["j1"].model_dump()) == before

    asyncio.run(scenario())


@pytest.mark.parametrize("execution", [False, True])
def test_timeout_checks_wait_for_store_lock(store, execution):
    async def scenario():
        if execution:
            store.leases["l1"].status = LeaseStatus.ACTIVE
            store.leases["l1"].expires_at = NOW
            store.jobs["j1"].status = JobStatus.RUNNING
            check = lease_service.expire_execution_leases(store, NOW, SETTINGS.max_attempts)
        else:
            check = lease_service.expire_offers(
                store, NOW, SETTINGS.max_attempts, SETTINGS.offer_timeouts_before_exclude
            )
        async with store.lock:
            before = store.leases["l1"].model_dump(), store.jobs["j1"].model_dump()
            task = asyncio.create_task(check)
            await asyncio.sleep(0)
            assert not task.done()
            assert (store.leases["l1"].model_dump(), store.jobs["j1"].model_dump()) == before
        await asyncio.wait_for(task, timeout=1)
        assert store.leases["l1"].status == LeaseStatus.EXPIRED

    asyncio.run(scenario())


def test_monitor_reclaims_draining_work_before_execution_timeout(store):
    provider = store.providers["p1"]
    provider.status = ProviderStatus.DRAINING
    provider.drain_deadline = NOW
    store.leases["l1"].status = LeaseStatus.ACTIVE
    store.leases["l1"].expires_at = NOW
    store.jobs["j1"].status = JobStatus.RUNNING
    store.jobs["j1"].attempt_count = SETTINGS.max_attempts

    asyncio.run(run_monitor_tick(store, NOW, SETTINGS))

    assert provider.status == ProviderStatus.DRAINED
    assert store.leases["l1"].status == LeaseStatus.REVOKED
    assert store.leases["l1"].reason == "DRAIN_RECLAIM"
    assert store.jobs["j1"].status == JobStatus.FAILED
    assert store.jobs["j1"].current_lease_id is None
