"""Heartbeat API scaffold tests. Fakes stand in for the pending service logic."""

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.models.provider import HeartbeatRequest, Provider, ProviderStatus
from coordinator.services import provider_service

NOW = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)
PROVIDER_TIME = datetime(2040, 1, 1, tzinfo=timezone.utc)
URL = "/providers/provider-a/heartbeat"


@pytest.fixture
def client():
    with TestClient(create_app(clock=lambda: NOW)) as test_client:
        yield test_client


@pytest.mark.parametrize("body", [None, {}, {"provider_time": PROVIDER_TIME.isoformat()}])
@pytest.mark.parametrize("returned_status", [ProviderStatus.ACTIVE, ProviderStatus.DRAINING])
def test_route_passes_store_path_parsed_body_and_coordinator_time(
    client, monkeypatch, body, returned_status
):
    calls = []

    async def fake_heartbeat(store, provider_id, request, received_at):
        calls.append((store, provider_id, request, received_at))
        return Provider(
            provider_id=provider_id,
            status=returned_status,
            capabilities={
                "gpu_model": "A100",
                "vram_mb": 40960,
                "driver_version": "550",
                "runtimes": ["cuda12"],
            },
            accepted_tiers=["tier1"],
            registered_at=received_at,
            last_heartbeat_at=received_at,
        )

    monkeypatch.setattr(provider_service, "heartbeat", fake_heartbeat)
    response = client.post(URL) if body is None else client.post(URL, json=body)

    assert len(calls) == 1
    store, provider_id, request, received_at = calls[0]
    assert store is client.app.state.store
    assert provider_id == "provider-a"
    assert isinstance(request, HeartbeatRequest)
    assert request.provider_time == (PROVIDER_TIME if body and "provider_time" in body else None)
    assert received_at == NOW
    assert response.status_code == 200
    assert response.json() == {
        "provider_id": "provider-a",
        "status": returned_status.value,
        "server_received_at": "2026-10-03T12:00:00Z",
    }


@pytest.mark.parametrize(
    "exception, status_code, error_code, message",
    [
        (provider_service.ProviderNotFound, 404, "NOT_FOUND", "provider_id 'provider-a' not found"),
        (
            provider_service.ProviderDrained,
            409,
            "INVALID_STATE_TRANSITION",
            "provider_id 'provider-a' is DRAINED",
        ),
    ],
)
def test_route_translates_domain_errors(
    client, monkeypatch, exception, status_code, error_code, message
):
    async def fake_heartbeat(store, provider_id, request, received_at):
        raise exception(provider_id)

    monkeypatch.setattr(provider_service, "heartbeat", fake_heartbeat)
    response = client.post(URL)

    assert response.status_code == status_code
    assert response.json() == {"error": {"code": error_code, "message": message}}


@pytest.mark.parametrize("body", [{"provider_time": "bad-time"}, {"status": "ACTIVE"}, []])
def test_invalid_body_is_rejected_before_service(client, monkeypatch, body):
    async def fake_heartbeat(*args):
        pytest.fail("Invalid input must not reach the service")

    monkeypatch.setattr(provider_service, "heartbeat", fake_heartbeat)
    response = client.post(URL, json=body)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


def test_openapi_exposes_heartbeat_contract(client):
    operation = client.get("/openapi.json").json()["paths"]["/providers/{provider_id}/heartbeat"][
        "post"
    ]
    assert operation["requestBody"].get("required", False) is False
    assert {"200", "422"} <= operation["responses"].keys()
