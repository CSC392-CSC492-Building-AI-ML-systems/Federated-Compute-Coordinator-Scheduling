import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from coordinator.models.job import Job, JobStatus
from coordinator.models.lease import TERMINAL_LEASE_STATUSES, Lease, LeaseStatus
from coordinator.models.provider import Capabilities, Provider, ProviderStatus
from coordinator.monitor import run_monitor_tick
from coordinator.services.provider_service import expire_heartbeats
from coordinator.settings import Settings
from coordinator.store import Store

NOW = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def store():
    state = Store()
    state.providers["p"] = Provider(
        provider_id="p",
        status=ProviderStatus.ACTIVE,
        capabilities=Capabilities(
            gpu_model="A100", vram_mb=40960, driver_version="550", runtimes=["cuda12"]
        ),
        accepted_tiers=["tier1"],
        registered_at=NOW - timedelta(seconds=30),
        last_heartbeat_at=NOW - timedelta(seconds=16),
    )
    state.jobs["j"] = Job(
        job_id="j",
        status=JobStatus.QUEUED,
        required_vram_mb=16384,
        gpu_models=[],
        required_runtime="cuda12",
        tier="tier1",
        current_lease_id="l",
        created_at=NOW,
    )
    state.leases["l"] = Lease(
        lease_id="l",
        job_id="j",
        provider_id="p",
        status=LeaseStatus.OFFERED,
        offer_deadline=NOW + timedelta(seconds=10),
        created_at=NOW,
    )
    return state


@pytest.mark.parametrize("provider_status", [ProviderStatus.ACTIVE, ProviderStatus.DRAINING])
@pytest.mark.parametrize("job_status", [JobStatus.QUEUED, JobStatus.STARTING, JobStatus.RUNNING])
def test_timeout_revokes_and_requeues_without_changing_attempts(store, provider_status, job_status):
    provider, job, lease = store.providers["p"], store.jobs["j"], store.leases["l"]
    provider.status = provider_status
    job.status = job_status
    job.attempt_count = 0 if job_status == JobStatus.QUEUED else 2
    lease.status = LeaseStatus.OFFERED if job_status == JobStatus.QUEUED else LeaseStatus.ACTIVE
    attempts = job.attempt_count

    asyncio.run(expire_heartbeats(store, NOW, 15))

    expected = (
        ProviderStatus.STALE if provider_status == ProviderStatus.ACTIVE else ProviderStatus.DRAINED
    )
    assert provider.status == expected
    assert lease.status == LeaseStatus.REVOKED
    assert lease.reason == "PROVIDER_STALE"
    assert lease.outcome is None
    assert job.status == JobStatus.QUEUED
    assert job.current_lease_id is None
    assert job.attempt_count == attempts
    assert job.last_failure_reason == (None if job_status == JobStatus.QUEUED else "PROVIDER_STALE")
    assert job.rejected_provider_ids == set()

    # Another tick must not change terminal records or count another attempt.
    before = job.model_dump(), lease.model_dump()
    asyncio.run(expire_heartbeats(store, NOW, 15))
    assert (job.model_dump(), lease.model_dump()) == before


@pytest.mark.parametrize("age", [0, 14.9, 15])
@pytest.mark.parametrize("provider_status", [ProviderStatus.ACTIVE, ProviderStatus.DRAINING])
def test_fresh_heartbeat_including_boundary_is_unchanged(store, age, provider_status):
    store.providers["p"].status = provider_status
    store.providers["p"].last_heartbeat_at = NOW - timedelta(seconds=age)
    before = store.providers["p"].model_dump(), store.leases["l"].model_dump()
    asyncio.run(expire_heartbeats(store, NOW, 15))
    assert (store.providers["p"].model_dump(), store.leases["l"].model_dump()) == before
    assert store.jobs["j"].current_lease_id == "l"


@pytest.mark.parametrize("lease_status", list(TERMINAL_LEASE_STATUSES))
def test_terminal_lease_is_preserved(store, lease_status):
    store.leases["l"].status = lease_status
    before = store.leases["l"].model_dump(), store.jobs["j"].model_dump()
    asyncio.run(expire_heartbeats(store, NOW, 15))
    assert (store.leases["l"].model_dump(), store.jobs["j"].model_dump()) == before


def test_old_lease_does_not_change_job_with_new_current_lease(store):
    store.jobs["j"].current_lease_id = "new-lease"
    store.jobs["j"].status = JobStatus.RUNNING
    before = store.jobs["j"].model_dump()
    asyncio.run(expire_heartbeats(store, NOW, 15))
    assert store.leases["l"].status == LeaseStatus.REVOKED
    assert store.jobs["j"].model_dump() == before


def test_other_provider_lease_is_preserved(store):
    store.leases["l"].provider_id = "other-provider"
    before = store.leases["l"].model_dump(), store.jobs["j"].model_dump()
    asyncio.run(expire_heartbeats(store, NOW, 15))
    assert (store.leases["l"].model_dump(), store.jobs["j"].model_dump()) == before


def test_multiple_expired_providers_leave_fresh_provider_work_untouched(store):
    for suffix, status, age in [
        ("2", ProviderStatus.DRAINING, 20),
        ("3", ProviderStatus.ACTIVE, 5),
    ]:
        store.providers[f"p{suffix}"] = store.providers["p"].model_copy(
            deep=True,
            update={
                "provider_id": f"p{suffix}",
                "status": status,
                "last_heartbeat_at": NOW - timedelta(seconds=age),
            },
        )
        store.jobs[f"j{suffix}"] = store.jobs["j"].model_copy(
            deep=True,
            update={
                "job_id": f"j{suffix}",
                "status": JobStatus.RUNNING,
                "current_lease_id": f"l{suffix}",
                "attempt_count": 1,
            },
        )
        store.leases[f"l{suffix}"] = store.leases["l"].model_copy(
            deep=True,
            update={
                "lease_id": f"l{suffix}",
                "provider_id": f"p{suffix}",
                "job_id": f"j{suffix}",
                "status": LeaseStatus.ACTIVE,
            },
        )

    before = store.jobs["j3"].model_dump(), store.leases["l3"].model_dump()
    asyncio.run(expire_heartbeats(store, NOW, 15))

    assert store.providers["p"].status == ProviderStatus.STALE
    assert store.providers["p2"].status == ProviderStatus.DRAINED
    assert store.leases["l"].status == store.leases["l2"].status == LeaseStatus.REVOKED
    assert store.jobs["j2"].status == JobStatus.QUEUED
    assert store.jobs["j2"].attempt_count == 1
    assert store.jobs["j2"].current_lease_id is None
    assert store.providers["p3"].status == ProviderStatus.ACTIVE
    assert (store.jobs["j3"].model_dump(), store.leases["l3"].model_dump()) == before


@pytest.mark.parametrize("provider_status", [ProviderStatus.STALE, ProviderStatus.DRAINED])
def test_inactive_provider_is_unchanged(store, provider_status):
    store.providers["p"].status = provider_status
    before = tuple(
        record.model_dump() for record in (store.providers["p"], store.jobs["j"], store.leases["l"])
    )

    asyncio.run(expire_heartbeats(store, NOW, 15))

    assert (
        tuple(
            record.model_dump()
            for record in (store.providers["p"], store.jobs["j"], store.leases["l"])
        )
        == before
    )


@pytest.mark.parametrize("provider_status", [ProviderStatus.ACTIVE, ProviderStatus.DRAINING])
def test_timeout_without_leases_still_updates_provider(store, provider_status):
    store.providers["p"].status = provider_status
    store.jobs.clear()
    store.leases.clear()

    asyncio.run(expire_heartbeats(store, NOW, 15))

    expected = (
        ProviderStatus.STALE if provider_status == ProviderStatus.ACTIVE else ProviderStatus.DRAINED
    )
    assert store.providers["p"].status == expected


@pytest.mark.parametrize("provider_status", [ProviderStatus.ACTIVE, ProviderStatus.DRAINING])
def test_monitor_tick_reclaims_stale_work_before_drain_check(store, provider_status):
    store.providers["p"].status = provider_status
    store.providers["p"].drain_deadline = NOW - timedelta(seconds=1)
    store.jobs["j"].status = JobStatus.RUNNING
    store.jobs["j"].attempt_count = 1
    store.leases["l"].status = LeaseStatus.ACTIVE
    settings = Settings(heartbeat_timeout_seconds=15, monitor_interval_seconds=1)

    asyncio.run(run_monitor_tick(store, NOW, settings))

    assert store.leases["l"].status == LeaseStatus.REVOKED
    assert store.leases["l"].reason == "PROVIDER_STALE"
    assert store.jobs["j"].status == JobStatus.QUEUED
    assert store.jobs["j"].current_lease_id is None
    assert store.jobs["j"].attempt_count == 1


def test_expiration_waits_for_store_lock(store):
    async def scenario():
        async with store.lock:
            task = asyncio.create_task(expire_heartbeats(store, NOW, 15))
            await asyncio.sleep(0)
            assert not task.done()
            assert store.providers["p"].status == ProviderStatus.ACTIVE
            assert store.leases["l"].status == LeaseStatus.OFFERED
            assert store.jobs["j"].current_lease_id == "l"
        await asyncio.wait_for(task, timeout=1)
        assert store.providers["p"].status == ProviderStatus.STALE
        assert store.leases["l"].status == LeaseStatus.REVOKED
        assert store.jobs["j"].current_lease_id is None

    asyncio.run(scenario())
