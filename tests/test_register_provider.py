"""POST /providers, tested layer by layer.

One fake request goes through each layer, and each layer's output is checked:

    fake JSON --> [1 model] --> [2 service + Store] --> [3 API route] --> HTTP response

1. Model:   JSON dict  -> ProviderRegisterRequest        (validation only)
2. Service: request    -> stored Provider + returned copy (business rules, no HTTP)
3. API:     HTTP call  -> right service call + response   (service replaced by a fake)
4. End to end: all layers together through HTTP
"""

import asyncio
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from coordinator.app import create_app
from coordinator.models.provider import (
    Provider,
    ProviderRegisterRequest,
    ProviderStatus,
)
from coordinator.services import provider_service
from coordinator.store import Store

# Fixed "Coordinator clock" so timestamps can be asserted exactly.
NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)


def register_body(**overrides):
    """The fake input: what a Provider would send as JSON."""
    body = {
        "provider_id": "provider-a",
        "capabilities": {
            "gpu_model": "A100",
            "vram_mb": 40960,
            "driver_version": "550.54",
            "runtimes": ["cuda12"],
        },
        "accepted_tiers": ["tier1"],
    }
    body.update(overrides)
    return body


@pytest.fixture
def client():
    # `with` runs the app lifespan, so every test gets a fresh Store and a fixed clock.
    with TestClient(create_app(clock=lambda: NOW)) as test_client:
        yield test_client


def stored_providers(client):
    return client.app.state.store.providers


# --- 1. Model layer: JSON -> request object ---------------------------------


def test_layer1_model_parses_fake_input():
    request = ProviderRegisterRequest(**register_body())

    assert request.provider_id == "provider-a"
    assert request.capabilities.gpu_model == "A100"
    assert request.capabilities.vram_mb == 40960
    assert request.accepted_tiers == ["tier1"]


# --- 2. Service layer: request -> Store -------------------------------------
# The Store is only three dicts and a lock, so a fresh real Store is the simplest
# "simulated" Store: no mock needed, and we can inspect it directly afterwards.
# Each test builds its Store inside the coroutine: on Python 3.9, asyncio.Lock()
# must be created while an event loop is running.


def test_layer2_service_stores_active_provider_and_returns_it():
    async def scenario():
        store = Store()
        request = ProviderRegisterRequest(**register_body())
        returned = await provider_service.register_provider(store, request, NOW)
        return store, returned

    store, returned = asyncio.run(scenario())

    # Output 1: what the service returns to the route.
    assert returned.provider_id == "provider-a"
    assert returned.status is ProviderStatus.ACTIVE
    assert returned.registered_at == NOW
    assert returned.last_heartbeat_at == NOW
    assert returned.drain_deadline is None
    # Output 2: what ended up in the Store.
    assert list(store.providers) == ["provider-a"]
    assert store.providers["provider-a"] == returned


def test_layer2_service_returns_copy_not_stored_record():
    async def scenario():
        store = Store()
        request = ProviderRegisterRequest(**register_body())
        returned = await provider_service.register_provider(store, request, NOW)
        return store, returned

    store, returned = asyncio.run(scenario())
    returned.status = ProviderStatus.DRAINED

    # Changing the returned object must not change shared state behind the lock.
    assert store.providers["provider-a"].status is ProviderStatus.ACTIVE


def test_layer2_service_rejects_duplicate_and_keeps_original():
    changed = register_body()
    changed["capabilities"]["vram_mb"] = 1

    async def scenario():
        store = Store()
        original = ProviderRegisterRequest(**register_body())
        await provider_service.register_provider(store, original, NOW)
        with pytest.raises(provider_service.ProviderAlreadyExists):
            await provider_service.register_provider(store, ProviderRegisterRequest(**changed), NOW)
        return store

    store = asyncio.run(scenario())

    assert store.providers["provider-a"].capabilities.vram_mb == 40960


# --- 3. API layer: HTTP -> service call -> HTTP response ---------------------
# The real service is replaced by a fake, so these tests check only what the route
# does: pass the right arguments, and turn the result or error into HTTP.


def test_layer3_route_passes_request_store_and_time_to_service(client, monkeypatch):
    calls = []

    async def fake_register_provider(store, request, received_at):
        calls.append((store, request, received_at))
        return Provider(
            provider_id=request.provider_id,
            status=ProviderStatus.ACTIVE,
            capabilities=request.capabilities,
            accepted_tiers=request.accepted_tiers,
            registered_at=received_at,
            last_heartbeat_at=received_at,
        )

    monkeypatch.setattr(provider_service, "register_provider", fake_register_provider)

    response = client.post("/providers", json=register_body())

    # Input side: the route called the service exactly once, with parsed input.
    assert len(calls) == 1
    store, request, received_at = calls[0]
    assert store is client.app.state.store
    assert isinstance(request, ProviderRegisterRequest)
    assert request.provider_id == "provider-a"
    assert received_at == NOW
    # Output side: the route turned the Provider into the contract's 201 body.
    assert response.status_code == 201
    assert response.json() == {
        "provider_id": "provider-a",
        "status": "ACTIVE",
        "registered_at": "2026-09-28T12:00:00Z",
        "last_heartbeat_at": "2026-09-28T12:00:00Z",
    }


def test_layer3_route_translates_duplicate_to_409(client, monkeypatch):
    async def fake_register_provider(store, request, received_at):
        raise provider_service.ProviderAlreadyExists(request.provider_id)

    monkeypatch.setattr(provider_service, "register_provider", fake_register_provider)

    response = client.post("/providers", json=register_body())

    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "DUPLICATE_ID",
            "message": "provider_id 'provider-a' already exists",
        }
    }


def test_layer3_invalid_input_never_reaches_service(client, monkeypatch):
    calls = []

    async def fake_register_provider(store, request, received_at):
        calls.append(request)

    monkeypatch.setattr(provider_service, "register_provider", fake_register_provider)

    response = client.post("/providers", json=register_body(provider_id=""))

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"
    assert calls == []


# --- 4. End to end: all real layers through HTTP -----------------------------


def test_e2e_register_returns_201_and_stores_provider(client):
    response = client.post("/providers", json=register_body())

    assert response.status_code == 201
    assert response.json()["status"] == "ACTIVE"
    provider = stored_providers(client)["provider-a"]
    assert provider.accepted_tiers == ["tier1"]
    assert provider.registered_at == provider.last_heartbeat_at == NOW


def test_e2e_duplicate_returns_409_and_keeps_original(client):
    client.post("/providers", json=register_body())
    changed = register_body()
    changed["capabilities"]["vram_mb"] = 1

    response = client.post("/providers", json=changed)

    assert response.status_code == 409
    assert stored_providers(client)["provider-a"].capabilities.vram_mb == 40960


@pytest.mark.parametrize(
    "body",
    [
        register_body(provider_id=""),
        register_body(unexpected="field"),
        {"provider_id": "provider-a", "accepted_tiers": ["tier1"]},
    ],
    ids=["empty-id", "extra-field", "missing-capabilities"],
)
def test_e2e_invalid_body_returns_422_and_stores_nothing(client, body):
    response = client.post("/providers", json=body)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"
    assert stored_providers(client) == {}
