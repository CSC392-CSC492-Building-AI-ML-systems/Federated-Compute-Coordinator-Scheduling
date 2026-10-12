import asyncio
from datetime import datetime, timedelta, timezone

from harness.errors import CoordinatorError
from harness.provider import FakeProvider

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


class StubClient:
    """Fake coordinator: always ACTIVE, hands out one offer, records calls."""

    def __init__(self, renew_error=None):
        self.calls = []
        self.renews = []
        self.offers = [{"lease_id": "lease-1", "job_id": "job-1"}]
        self.renew_error = renew_error

    async def register_provider(self, data):
        return {"status": "ACTIVE"}

    async def heartbeat(self, provider_id, provider_time=None):
        self.calls.append("heartbeat")
        return {"status": "ACTIVE"}

    async def get_lease_offers(self, provider_id):
        offers, self.offers = self.offers, []
        return offers

    async def accept(self, lease_id, provider_id):
        pass

    async def started(self, lease_id, provider_id):
        pass

    async def renew(self, lease_id, provider_id, extend_seconds):
        self.renews.append(extend_seconds)
        if self.renew_error:
            raise self.renew_error

    async def report(self, lease_id, provider_id, outcome, reason=None):
        self.calls.append("report")


def run(client, params, seconds):
    """Register at T0, then tick once per second for `seconds` seconds."""
    provider = FakeProvider(client, "p1", "A100", 40960, ["cuda12"], ["standard"], params=params)

    async def scenario():
        await provider.register(T0)
        for sec in range(1, seconds + 1):
            await provider.tick(T0 + timedelta(seconds=sec))

    asyncio.run(scenario())
    return provider


def test_heartbeat_interval_comes_from_params():
    client = StubClient()
    run(client, {"heartbeat_interval_sec": 2}, 6)
    # Heartbeats at t=1, 3, 5
    assert client.calls.count("heartbeat") == 3


def test_job_duration_comes_from_params():
    # Accepted at t=1 with a 7s job, so it finishes at t=8
    client = StubClient()
    run(client, {"duration_sec": 7}, 7)
    assert "report" not in client.calls

    client = StubClient()
    run(client, {"duration_sec": 7}, 8)
    assert "report" in client.calls


def test_renews_by_the_renew_interval():
    client = StubClient()
    run(client, {"renew_interval_sec": 3, "duration_sec": 8}, 9)
    # Accepted at t=1, renewed at t=4 and t=7 (each adds 3s), finished at t=9
    assert client.renews == [3, 3]
    assert "report" in client.calls


def test_failed_renew_drops_the_lease():
    client = StubClient(renew_error=CoordinatorError(409, "INVALID_STATE_TRANSITION", "expired"))
    provider = run(client, {"renew_interval_sec": 3, "duration_sec": 8}, 9)
    # The renew at t=4 fails, so the provider gives up the job and never reports it
    assert client.renews == [3]
    assert provider.lease_id is None
    assert "report" not in client.calls
