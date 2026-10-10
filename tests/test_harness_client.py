"""CoordinatorClient against the real coordinator app, in-process.

Covers only endpoints that exist on the coordinator so far, plus how the client
turns error responses and network failures into exceptions.
"""

import asyncio

import httpx
import pytest

from coordinator.app import create_app
from harness.client import CoordinatorClient
from harness.errors import CoordinatorError

PROVIDER = {
    "provider_id": "provider-a",
    "capabilities": {
        "gpu_model": "A100",
        "vram_mb": 40960,
        "driver_version": "550.54",
        "runtimes": ["cuda12"],
    },
    "accepted_tiers": ["tier1"],
}

JOB = {
    "required_vram_mb": 8192,
    "gpu_models": [],
    "required_runtime": "cuda12",
    "tier": "tier1",
}


def run_with_coordinator(test):
    """Run `test(client)` against a fresh coordinator app."""

    async def main():
        app = create_app()
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with CoordinatorClient("http://test", "key", transport) as client:
                return await test(client)

    return asyncio.run(main())


def run_with_transport(handler, test):
    """Run `test(client)` against a fake transport that answers with `handler`."""

    async def main():
        transport = httpx.MockTransport(handler)
        async with CoordinatorClient("http://test", "key", transport) as client:
            return await test(client)

    return asyncio.run(main())


def test_register_provider_returns_active():
    body = run_with_coordinator(lambda client: client.register_provider(PROVIDER))

    assert body["provider_id"] == "provider-a"
    assert body["status"] == "ACTIVE"


def test_duplicate_provider_raises_coordinator_error():
    async def test(client):
        await client.register_provider(PROVIDER)
        with pytest.raises(CoordinatorError) as caught:
            await client.register_provider(PROVIDER)
        return caught.value

    error = run_with_coordinator(test)

    assert error.status_code == 409
    assert error.code == "DUPLICATE_ID"


def test_submit_and_get_job():
    async def test(client):
        submitted = await client.submit_job(JOB)
        fetched = await client.get_job(submitted["job_id"])
        return submitted, fetched

    submitted, fetched = run_with_coordinator(test)

    assert submitted["status"] == "QUEUED"
    assert submitted["attempt_count"] == 0
    assert fetched["job_id"] == submitted["job_id"]


def test_missing_job_raises_job_not_found():
    async def test(client):
        with pytest.raises(CoordinatorError) as caught:
            await client.get_job("no-such-job")
        return caught.value

    error = run_with_coordinator(test)

    assert error.status_code == 404
    assert error.code == "JOB_NOT_FOUND"


def test_error_without_contract_shape_falls_back_to_unknown():
    def handler(request):
        return httpx.Response(500, text="Internal Server Error")

    async def test(client):
        with pytest.raises(CoordinatorError) as caught:
            await client.get_status()
        return caught.value

    error = run_with_transport(handler, test)

    assert error.status_code == 500
    assert error.code == "UNKNOWN"
    assert error.message == "Internal Server Error"


def test_network_failure_is_not_a_coordinator_error():
    def handler(request):
        raise httpx.ConnectError("connection refused", request=request)

    async def test(client):
        with pytest.raises(httpx.ConnectError):
            await client.heartbeat("provider-a")

    run_with_transport(handler, test)


def test_heartbeat_sends_provider_time_only_when_given():
    sent = []

    def handler(request):
        sent.append(request.content)
        return httpx.Response(200, json={"provider_id": "provider-a", "status": "ACTIVE"})

    async def test(client):
        await client.heartbeat("provider-a")
        await client.heartbeat("provider-a", "2026-10-04T12:00:00+00:00")

    run_with_transport(handler, test)

    assert sent == [b"{}", b'{"provider_time":"2026-10-04T12:00:00+00:00"}']
