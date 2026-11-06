"""Tests for POST /leases/{lease_id}/reject and reject_lease service logic."""

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.models.job import Job, JobStatus
from coordinator.models.lease import Lease, LeaseStatus, RejectReason, RejectRequest
from coordinator.models.provider import Capabilities, Provider, ProviderStatus
from coordinator.services.lease_service import (
    InvalidLeaseState,
    LeaseNotFound,
    LeaseProviderMismatch,
    create_lease,
    reject_lease,
)
from coordinator.store import Store

FIXED_NOW = datetime(2026, 11, 3, 12, 0, 0, tzinfo=timezone.utc)


def _make_provider(provider_id: str = "prov-1") -> Provider:
    return Provider(
        provider_id=provider_id,
        status=ProviderStatus.ACTIVE,
        capabilities=Capabilities(
            gpu_model="NVIDIA RTX 4090",
            vram_mb=24576,
            driver_version="550.54",
            runtimes=["cuda", "docker", "pytorch"],
        ),
        accepted_tiers=["standard"],
        registered_at=FIXED_NOW,
        last_heartbeat_at=FIXED_NOW,
    )


def _make_job(job_id: str = "job-1") -> Job:
    return Job(
        job_id=job_id,
        status=JobStatus.QUEUED,
        required_vram_mb=8192,
        gpu_models=["NVIDIA RTX 4090"],
        required_runtime="cuda",
        tier="standard",
        created_at=FIXED_NOW,
    )


def _seed_offered_lease(
    store: Store,
    provider_id: str = "prov-1",
    job_id: str = "job-1",
) -> tuple[Provider, Job, Lease]:
    """Seed a provider, a QUEUED job, and an OFFERED lease linked to that job."""
    provider = _make_provider(provider_id)
    job = _make_job(job_id)
    store.providers[provider.provider_id] = provider
    store.jobs[job.job_id] = job

    lease = create_lease(
        store,
        job_id=job.job_id,
        provider_id=provider.provider_id,
        now=FIXED_NOW,
        timeout=60.0,
    )
    job.current_lease_id = lease.lease_id
    return provider, job, lease


@pytest.fixture
def client():
    app = create_app(clock=lambda: FIXED_NOW)
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# Service-Layer Unit Tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_service_reject_lease_happy_path():
    store = Store()
    _, job, lease = _seed_offered_lease(store, provider_id="prov-1", job_id="job-1")

    req = RejectRequest(provider_id="prov-1", reason=RejectReason.LOCAL_BUSY)
    returned = await reject_lease(store, lease.lease_id, req, now=FIXED_NOW)

    # Returned RejectResponse fields
    assert returned.lease_id == lease.lease_id
    assert returned.lease_status == LeaseStatus.REJECTED
    assert returned.job_status == JobStatus.QUEUED.value

    # Store Lease state
    stored_lease = store.leases[lease.lease_id]
    assert stored_lease.status == LeaseStatus.REJECTED
    assert stored_lease.reason == RejectReason.LOCAL_BUSY.value
    assert stored_lease.outcome is None

    # Store Job state: provider added to rejected_provider_ids, still QUEUED, 0 attempts used
    stored_job = store.jobs[job.job_id]
    assert "prov-1" in stored_job.rejected_provider_ids
    assert stored_job.current_lease_id is None
    assert stored_job.status == JobStatus.QUEUED
    assert stored_job.attempt_count == 0
    assert stored_job.last_failure_reason is None

    # Verify response mutation does not affect stored state
    returned.lease_status = LeaseStatus.OFFERED
    assert store.leases[lease.lease_id].status == LeaseStatus.REJECTED


@pytest.mark.anyio
async def test_service_reject_lease_not_found():
    store = Store()
    req = RejectRequest(provider_id="prov-1", reason=RejectReason.LOCAL_BUSY)

    with pytest.raises(LeaseNotFound):
        await reject_lease(store, "lease-does-not-exist", req, now=FIXED_NOW)


@pytest.mark.anyio
async def test_service_reject_lease_provider_mismatch():
    store = Store()
    _, job, lease = _seed_offered_lease(store, provider_id="prov-1", job_id="job-1")

    req = RejectRequest(provider_id="prov-intruder", reason=RejectReason.LOCAL_BUSY)
    with pytest.raises(LeaseProviderMismatch):
        await reject_lease(store, lease.lease_id, req, now=FIXED_NOW)

    # State must remain untouched
    assert store.leases[lease.lease_id].status == LeaseStatus.OFFERED
    assert store.jobs[job.job_id].current_lease_id == lease.lease_id
    assert len(store.jobs[job.job_id].rejected_provider_ids) == 0


@pytest.mark.anyio
async def test_service_reject_lease_invalid_state_when_active():
    store = Store()
    _, job, lease = _seed_offered_lease(store, provider_id="prov-1", job_id="job-1")

    # Mutate the authoritative objects in the store
    store.leases[lease.lease_id].status = LeaseStatus.ACTIVE
    store.jobs[job.job_id].status = JobStatus.RUNNING
    store.jobs[job.job_id].attempt_count = 1

    req = RejectRequest(provider_id="prov-1", reason=RejectReason.LOCAL_BUSY)
    with pytest.raises(InvalidLeaseState):
        await reject_lease(store, lease.lease_id, req, now=FIXED_NOW)

    # Active lease and running job must not be disrupted
    assert store.leases[lease.lease_id].status == LeaseStatus.ACTIVE
    assert store.jobs[job.job_id].current_lease_id == lease.lease_id
    assert store.jobs[job.job_id].status == JobStatus.RUNNING
    assert "prov-1" not in store.jobs[job.job_id].rejected_provider_ids


# ---------------------------------------------------------------------------
# HTTP API Tests
# ---------------------------------------------------------------------------


def test_api_reject_lease_200_and_removes_from_offers(client: TestClient):
    store: Store = client.app.state.store
    _, job, lease = _seed_offered_lease(store, provider_id="prov-1", job_id="job-1")

    resp = client.post(
        f"/leases/{lease.lease_id}/reject",
        json={"provider_id": "prov-1", "reason": "LOCAL_BUSY"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body == {
        "lease_id": lease.lease_id,
        "lease_status": "REJECTED",
        "job_status": "QUEUED",
    }

    # Store Lease & Job invariants
    assert store.leases[lease.lease_id].status == LeaseStatus.REJECTED
    assert store.leases[lease.lease_id].reason == "LOCAL_BUSY"
    assert "prov-1" in store.jobs[job.job_id].rejected_provider_ids
    assert store.jobs[job.job_id].current_lease_id is None
    assert store.jobs[job.job_id].status == JobStatus.QUEUED
    assert store.jobs[job.job_id].attempt_count == 0

    # Integration check: rejected lease no longer shows up in GET /providers/{id}/lease-offers
    offers_resp = client.get("/providers/prov-1/lease-offers")
    assert offers_resp.status_code == 200
    assert offers_resp.json()["offers"] == []


def test_api_reject_lease_404_when_missing(client: TestClient):
    resp = client.post(
        "/leases/nonexistent-lease/reject",
        json={"provider_id": "prov-1", "reason": "LOCAL_BUSY"},
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "LEASE_NOT_FOUND"


def test_api_reject_lease_403_on_provider_mismatch(client: TestClient):
    store: Store = client.app.state.store
    _, job, lease = _seed_offered_lease(store, provider_id="prov-1", job_id="job-1")

    resp = client.post(
        f"/leases/{lease.lease_id}/reject",
        json={"provider_id": "prov-other", "reason": "LOCAL_BUSY"},
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "LEASE_PROVIDER_MISMATCH"
    assert store.leases[lease.lease_id].status == LeaseStatus.OFFERED
    assert store.jobs[job.job_id].current_lease_id == lease.lease_id


def test_api_reject_lease_409_when_rejected_twice(client: TestClient):
    store: Store = client.app.state.store
    _, _, lease = _seed_offered_lease(store, provider_id="prov-1", job_id="job-1")

    first = client.post(
        f"/leases/{lease.lease_id}/reject",
        json={"provider_id": "prov-1", "reason": "LOCAL_BUSY"},
    )
    assert first.status_code == 200

    second = client.post(
        f"/leases/{lease.lease_id}/reject",
        json={"provider_id": "prov-1", "reason": "LOCAL_BUSY"},
    )
    assert second.status_code == 409
    assert second.json()["error"]["code"] == "INVALID_LEASE_STATE"


def test_api_reject_lease_409_when_active(client: TestClient):
    store: Store = client.app.state.store
    _, job, lease = _seed_offered_lease(store, provider_id="prov-1", job_id="job-1")

    # Mutate the authoritative objects in the store
    store.leases[lease.lease_id].status = LeaseStatus.ACTIVE
    store.jobs[job.job_id].status = JobStatus.RUNNING

    resp = client.post(
        f"/leases/{lease.lease_id}/reject",
        json={"provider_id": "prov-1", "reason": "LOCAL_BUSY"},
    )
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "INVALID_LEASE_STATE"


@pytest.mark.parametrize(
    "bad_payload",
    [
        {"provider_id": "prov-1", "reason": "INVALID_REASON_ENUM"},
        {"provider_id": "", "reason": "LOCAL_BUSY"},
        {"provider_id": "prov-1", "reason": "LOCAL_BUSY", "extra_field": 123},
    ],
)
def test_api_reject_lease_422_on_invalid_body(client: TestClient, bad_payload: dict):
    store: Store = client.app.state.store
    _, _, lease = _seed_offered_lease(store, provider_id="prov-1", job_id="job-1")

    resp = client.post(f"/leases/{lease.lease_id}/reject", json=bad_payload)
    assert resp.status_code == 422
