"""The job and lease tables keep short lists of unfinished records, so loops stay fast."""

import asyncio
import random
from datetime import datetime, timezone

import pytest

from coordinator.models.job import Job, JobStatus
from coordinator.models.lease import TERMINAL_LEASE_STATUSES, Lease, LeaseStatus
from coordinator.models.provider import Capabilities, Provider, ProviderStatus
from coordinator.scheduler import run_matching
from coordinator.services.lease_service import get_provider_lease_offers
from coordinator.settings import load_settings
from coordinator.store import JobTable, LeaseTable, Store

NOW = datetime(2026, 11, 27, 12, 0, tzinfo=timezone.utc)
FINISHED = sorted(TERMINAL_LEASE_STATUSES)
# Written out here, so a mistake in JobTable cannot also change what the tests expect.
FINISHED_JOBS = [JobStatus.COMPLETED, JobStatus.FAILED]

# How a job may move on. A finished job never moves again.
JOB_MOVES = {
    JobStatus.QUEUED: [JobStatus.STARTING, JobStatus.FAILED],
    JobStatus.STARTING: [JobStatus.RUNNING, JobStatus.QUEUED, JobStatus.FAILED],
    JobStatus.RUNNING: [JobStatus.COMPLETED, JobStatus.QUEUED, JobStatus.FAILED],
}


def make_lease(lease_id, provider_id, status=LeaseStatus.OFFERED, job_id="job-1"):
    return Lease(
        lease_id=lease_id,
        job_id=job_id,
        provider_id=provider_id,
        status=status,
        offer_deadline=NOW,
        created_at=NOW,
    )


def make_provider(provider_id):
    return Provider(
        provider_id=provider_id,
        status=ProviderStatus.ACTIVE,
        capabilities=Capabilities(
            gpu_model="A100", vram_mb=40960, driver_version="550.54", runtimes=["cuda12"]
        ),
        accepted_tiers=["standard"],
        registered_at=NOW,
        last_heartbeat_at=NOW,
    )


def make_job(job_id, status=JobStatus.QUEUED):
    return Job(
        job_id=job_id,
        status=status,
        created_at=NOW,
        required_vram_mb=1024,
        gpu_models=[],
        required_runtime="cuda12",
        tier="standard",
    )


def ids(leases):
    return [lease.lease_id for lease in leases]


def job_ids(jobs):
    return [job.job_id for job in jobs]


def test_a_lease_stored_directly_is_listed_for_its_provider_only():
    # Tests store leases with table[id] = lease, so that write must file the lease too.
    table = LeaseTable()
    table["l1"] = make_lease("l1", "p1")

    assert ids(table.unfinished_for("p1")) == ["l1"]
    assert table.unfinished_for("p2") == []
    assert ids(table.unfinished()) == ["l1"]


def test_leases_are_listed_in_the_order_they_were_stored():
    table = LeaseTable()
    table["l2"] = make_lease("l2", "p1")
    table["l1"] = make_lease("l1", "p1", status=LeaseStatus.ACTIVE)
    table["l3"] = make_lease("l3", "p1")

    assert ids(table.unfinished_for("p1")) == ["l2", "l1", "l3"]
    assert ids(table.unfinished()) == ["l2", "l1", "l3"]


def test_jobs_are_listed_in_the_order_they_were_stored():
    # The scheduler hands out work in this order, so the oldest job must come first.
    table = JobTable()
    table["j2"] = make_job("j2")
    table["j1"] = make_job("j1", status=JobStatus.RUNNING)
    table["j3"] = make_job("j3", status=JobStatus.STARTING)

    assert job_ids(table.unfinished()) == ["j2", "j1", "j3"]


def test_storing_the_same_lease_twice_lists_it_once():
    table = LeaseTable()
    table["l1"] = make_lease("l1", "p1")
    table["l1"] = make_lease("l1", "p1")

    assert ids(table.unfinished_for("p1")) == ["l1"]
    assert ids(table.unfinished()) == ["l1"]


@pytest.mark.parametrize("status", FINISHED)
def test_a_finished_lease_leaves_the_lists_for_good(status):
    table = LeaseTable()
    table["l1"] = make_lease("l1", "p1")
    table["l1"].status = status

    assert table.unfinished_for("p1") == []
    assert table.unfinished() == []
    # Forgetting finished leases is what keeps the lists short as history grows.
    assert table._ids_by_provider["p1"] == {}
    assert table._unfinished_ids == {}


@pytest.mark.parametrize("status", FINISHED_JOBS)
def test_a_finished_job_leaves_the_list_for_good(status):
    table = JobTable()
    table["j1"] = make_job("j1")
    table["j1"].status = status

    assert table.unfinished() == []
    assert table._unfinished_ids == {}


def test_offered_and_running_leases_stay_on_the_lists():
    table = LeaseTable()
    table["l1"] = make_lease("l1", "p1")
    table["l2"] = make_lease("l2", "p1", status=LeaseStatus.ACTIVE)

    # Asking twice must not drop anything that is still open.
    table.unfinished_for("p1")
    table.unfinished()
    assert ids(table.unfinished_for("p1")) == ["l1", "l2"]
    assert ids(table.unfinished()) == ["l1", "l2"]


@pytest.mark.parametrize("status", sorted(JOB_MOVES))
def test_queued_and_running_jobs_stay_on_the_list(status):
    table = JobTable()
    table["j1"] = make_job("j1", status=status)

    table.unfinished()
    assert job_ids(table.unfinished()) == ["j1"]


def test_removing_leases_from_the_table_does_not_break_the_lists():
    table = LeaseTable()
    table["l1"] = make_lease("l1", "p1")
    table["l2"] = make_lease("l2", "p1")
    del table["l1"]
    assert ids(table.unfinished_for("p1")) == ["l2"]
    assert ids(table.unfinished()) == ["l2"]

    table.clear()
    assert table.unfinished_for("p1") == []
    assert table.unfinished() == []


@pytest.mark.parametrize(
    ("table", "record"),
    [(JobTable(), make_job("r1")), (LeaseTable(), make_lease("r1", "p1"))],
)
def test_writes_that_skip_the_lists_are_refused(table, record):
    with pytest.raises(TypeError):
        table.update({"r1": record})
    with pytest.raises(TypeError):
        table.setdefault("r1", record)
    with pytest.raises(TypeError):
        table |= {"r1": record}
    assert len(table) == 0


@pytest.mark.parametrize("seed", range(5))
def test_lists_match_looking_through_every_record(seed):
    # The old loops looked through every record ever made.
    # The lists must give the same records in the same order while records come and finish.
    rng = random.Random(seed)
    store = Store()
    providers = [f"p{i}" for i in range(6)]
    for provider_id in providers:
        store.providers[provider_id] = make_provider(provider_id)

    for step in range(400):
        roll = rng.random()
        if roll < 0.3:
            # 1, a new job, and an offer for it to a random provider
            store.jobs[f"j{step}"] = make_job(f"j{step}")
            lease = make_lease(f"l{step}", rng.choice(providers), job_id=f"j{step}")
            store.leases[lease.lease_id] = lease
        elif roll < 0.5 and store.leases:
            # 2, a lease moves on, offered -> running or finished, running -> finished
            lease = store.leases[rng.choice(list(store.leases))]
            if lease.status == LeaseStatus.OFFERED:
                lease.status = rng.choice([LeaseStatus.ACTIVE, *FINISHED])
            elif lease.status == LeaseStatus.ACTIVE:
                lease.status = rng.choice(FINISHED)
        elif roll < 0.7 and store.jobs:
            # 3, a job moves on, and a finished job stays where it is
            job = store.jobs[rng.choice(list(store.jobs))]
            if job.status in JOB_MOVES:
                job.status = rng.choice(JOB_MOVES[job.status])
        else:
            # 4, every list is checked against a look at every record
            provider_id = rng.choice(providers)
            expected_offers = [
                lease.lease_id
                for lease in store.leases.values()
                if lease.provider_id == provider_id and lease.status == LeaseStatus.OFFERED
            ]
            result = asyncio.run(get_provider_lease_offers(store, provider_id))
            assert [offer.lease.lease_id for offer in result.offers] == expected_offers

            expected_leases = [
                lease.lease_id
                for lease in store.leases.values()
                if lease.status not in TERMINAL_LEASE_STATUSES
            ]
            assert ids(store.leases.unfinished()) == expected_leases

            expected_jobs = [
                job.job_id for job in store.jobs.values() if job.status not in FINISHED_JOBS
            ]
            assert job_ids(store.jobs.unfinished()) == expected_jobs


@pytest.mark.parametrize("status", [LeaseStatus.OFFERED, LeaseStatus.ACTIVE, *FINISHED])
def test_the_scheduler_skips_a_provider_with_an_unfinished_lease(status):
    # The scheduler finds busy providers through the unfinished list.
    # A provider with an open lease must not get a second offer.
    store = Store()
    store.providers["p1"] = make_provider("p1")
    store.jobs["old"] = make_job("old", status=JobStatus.RUNNING)
    store.leases["l-old"] = make_lease("l-old", "p1", status=status, job_id="old")
    store.jobs["new"] = make_job("new")

    asyncio.run(run_matching(NOW, store, load_settings()))

    busy = status not in TERMINAL_LEASE_STATUSES
    assert (store.jobs["new"].current_lease_id is None) == busy
