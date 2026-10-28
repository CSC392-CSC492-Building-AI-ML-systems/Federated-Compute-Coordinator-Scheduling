"""Tests for GET /providers/{provider_id}/lease-offers across schema, service, and API."""

import asyncio
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from coordinator.api.tools.dependencies import get_store
from coordinator.app import app
from coordinator.models.job import Job, JobStatus
from coordinator.models.lease import (
    JobRequirements,
    Lease,
    LeaseOfferItem,
    LeaseStatus,
    ProviderLeaseOffersResponse,
)
from coordinator.models.provider import (
    Provider,
    ProviderRegisterRequest,
    ProviderStatus,
)
from coordinator.services.lease_service import (
    ProviderNotFound,
    get_provider_lease_offers,
)
from coordinator.store import Store

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)


# --- Helpers ----------------------------------------------------------------


def capabilities_body(**overrides):
    body = {
        "gpu_model": "A100",
        "vram_mb": 40960,
        "driver_version": "550.54",
        "runtimes": ["cuda12"],
    }
    body.update(overrides)
    return body


def register_body(**overrides):
    body = {
        "provider_id": "provider-a",
        "capabilities": capabilities_body(),
        "accepted_tiers": ["tier1"],
    }
    body.update(overrides)
    return body


def job_body(**overrides):
    body = {
        "required_vram_mb": 16384,
        "gpu_models": ["A100", "RTX4090"],
        "required_runtime": "cuda12",
        "tier": "tier1",
    }
    body.update(overrides)
    return body


def make_provider(**overrides):
    request = ProviderRegisterRequest(**register_body(**overrides))
    return Provider(
        **request.model_dump(),
        status=ProviderStatus.ACTIVE,
        registered_at=NOW,
        last_heartbeat_at=NOW,
    )


def make_job(**overrides):
    fields = {"job_id": "job-1", "status": JobStatus.STARTING, "created_at": NOW, **job_body()}
    fields.update(overrides)
    return Job(**fields)


def make_lease(**overrides):
    fields = {
        "lease_id": "lease-1",
        "job_id": "job-1",
        "provider_id": "provider-a",
        "status": LeaseStatus.OFFERED,
        "offer_deadline": NOW,
        "created_at": NOW,
    }
    fields.update(overrides)
    return Lease(**fields)


# --- Schema Tests -----------------------------------------------------------


def test_lease_offer_response_models_serialize_cleanly():
    lease = make_lease()
    reqs = JobRequirements(**job_body())
    item = LeaseOfferItem(lease=lease, requirements=reqs)
    response = ProviderLeaseOffersResponse(offers=[item])

    data = response.model_dump(mode="json")
    assert len(data["offers"]) == 1
    assert data["offers"][0]["lease"]["lease_id"] == "lease-1"
    assert data["offers"][0]["lease"]["status"] == "OFFERED"
    assert data["offers"][0]["requirements"] == {
        "required_vram_mb": 16384,
        "gpu_models": ["A100", "RTX4090"],
        "required_runtime": "cuda12",
        "tier": "tier1",
    }


# --- Service Layer Tests ----------------------------------------------------


def test_get_provider_lease_offers_raises_for_unknown_provider():
    store = Store()
    with pytest.raises(ProviderNotFound) as exc_info:
        asyncio.run(get_provider_lease_offers(store, "unknown-provider"))
    assert exc_info.value.provider_id == "unknown-provider"


def test_get_provider_lease_offers_returns_empty_list_when_none_exist():
    store = Store()
    store.providers["provider-a"] = make_provider(provider_id="provider-a")

    result = asyncio.run(get_provider_lease_offers(store, "provider-a"))
    assert result.offers == []


def test_get_provider_lease_offers_filters_by_provider_and_offered_status():
    store = Store()
    store.providers["provider-a"] = make_provider(provider_id="provider-a")
    store.providers["provider-b"] = make_provider(provider_id="provider-b")

    store.jobs["job-1"] = make_job(job_id="job-1", required_vram_mb=16384)
    store.jobs["job-2"] = make_job(job_id="job-2", required_vram_mb=32768)
    store.jobs["job-3"] = make_job(job_id="job-3", required_vram_mb=8192)

    # 1. Target lease: OFFERED for provider-a (Should be returned)
    store.leases["lease-1"] = make_lease(
        lease_id="lease-1", job_id="job-1", provider_id="provider-a", status=LeaseStatus.OFFERED
    )
    # 2. Wrong status: ACTIVE for provider-a (Should be ignored)
    store.leases["lease-2"] = make_lease(
        lease_id="lease-2", job_id="job-2", provider_id="provider-a", status=LeaseStatus.ACTIVE
    )
    # 3. Wrong provider: OFFERED for provider-b (Should be ignored)
    store.leases["lease-3"] = make_lease(
        lease_id="lease-3", job_id="job-3", provider_id="provider-b", status=LeaseStatus.OFFERED
    )

    result = asyncio.run(get_provider_lease_offers(store, "provider-a"))

    assert len(result.offers) == 1
    offer = result.offers[0]
    assert offer.lease.lease_id == "lease-1"
    assert offer.lease.status is LeaseStatus.OFFERED
    assert offer.requirements.required_vram_mb == 16384


def test_get_provider_lease_offers_returns_isolated_deep_copies():
    store = Store()
    store.providers["provider-a"] = make_provider(provider_id="provider-a")
    store.jobs["job-1"] = make_job(job_id="job-1", gpu_models=["A100"])
    store.leases["lease-1"] = make_lease(
        lease_id="lease-1", job_id="job-1", provider_id="provider-a", status=LeaseStatus.OFFERED
    )

    result = asyncio.run(get_provider_lease_offers(store, "provider-a"))

    # Mutate the returned DTOs
    result.offers[0].lease.status = LeaseStatus.ACTIVE
    result.offers[0].requirements.gpu_models.append("H100")

    # Verify the internal Store records remain untouched
    assert store.leases["lease-1"].status is LeaseStatus.OFFERED
    assert store.jobs["job-1"].gpu_models == ["A100"]


# --- API Endpoint Tests -----------------------------------------------------


def make_test_client(store: Store) -> TestClient:
    app.dependency_overrides[get_store] = lambda: store
    return TestClient(app)


def test_get_lease_offers_endpoint_returns_200_and_payload():
    store = Store()
    store.providers["provider-a"] = make_provider(provider_id="provider-a")
    store.jobs["job-1"] = make_job(
        job_id="job-1",
        required_vram_mb=24576,
        gpu_models=["RTX4090"],
        required_runtime="cuda12",
        tier="tier1",
    )
    store.leases["lease-1"] = make_lease(
        lease_id="lease-1", job_id="job-1", provider_id="provider-a", status=LeaseStatus.OFFERED
    )

    client = make_test_client(store)
    try:
        response = client.get("/providers/provider-a/lease-offers")
        assert response.status_code == 200

        body = response.json()
        assert len(body["offers"]) == 1
        assert body["offers"][0]["lease"]["lease_id"] == "lease-1"
        assert body["offers"][0]["requirements"] == {
            "required_vram_mb": 24576,
            "gpu_models": ["RTX4090"],
            "required_runtime": "cuda12",
            "tier": "tier1",
        }
    finally:
        app.dependency_overrides.clear()


def test_get_lease_offers_endpoint_returns_404_for_missing_provider():
    store = Store()
    client = make_test_client(store)
    try:
        response = client.get("/providers/missing-provider/lease-offers")
        assert response.status_code == 404
        body = response.json()
        assert "PROVIDER_NOT_FOUND" in str(body)
    finally:
        app.dependency_overrides.clear()
