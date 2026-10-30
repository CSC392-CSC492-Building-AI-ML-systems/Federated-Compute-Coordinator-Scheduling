import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from harness.behaviors import ACCEPT, Behavior, JobPlan, OfferDecision, make_behavior
from harness.provider import FakeProvider

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


class RecordingClient:
    """Fake coordinator: always ACTIVE, hands out one offer, records lease calls."""

    def __init__(self):
        self.offers = [{"lease_id": "lease-1", "job_id": "job-1"}]
        self.rejects = []
        self.reports = []

    async def register_provider(self, data):
        return {"status": "ACTIVE"}

    async def heartbeat(self, provider_id, provider_time=None):
        return {"status": "ACTIVE"}

    async def get_lease_offers(self, provider_id):
        offers, self.offers = self.offers, []
        return offers

    async def accept(self, lease_id, provider_id):
        pass

    async def reject(self, lease_id, provider_id, reason):
        self.rejects.append((lease_id, reason))

    async def started(self, lease_id, provider_id):
        pass

    async def renew(self, lease_id, provider_id, extend_seconds):
        pass

    async def report(self, lease_id, provider_id, outcome, reason=None):
        self.reports.append((lease_id, outcome, reason))


def run(client, seconds, behavior=None, params=None):
    """Register at T0, then tick once per second. `behavior` replaces the normal one."""
    provider = FakeProvider(client, "p1", "A100", 40960, ["cuda12"], ["standard"], params=params)
    if behavior is not None:
        provider.behavior = behavior

    async def scenario():
        await provider.register(T0)
        for sec in range(1, seconds + 1):
            await provider.tick(T0 + timedelta(seconds=sec))

    asyncio.run(scenario())
    return provider


def event_names(provider):
    return [event["event"] for event in provider.events]


# Registry


def test_make_behavior_builds_normal_with_params():
    behavior = make_behavior("normal", {"duration_sec": 5})
    assert type(behavior) is Behavior
    assert behavior.params == {"duration_sec": 5}


def test_planned_behavior_is_not_implemented_yet():
    with pytest.raises(NotImplementedError, match="disappear"):
        make_behavior("disappear", {})


def test_unknown_behavior_fails_fast():
    with pytest.raises(ValueError, match="nromal"):
        make_behavior("nromal", {})


# Normal hooks


def test_normal_hooks():
    behavior = Behavior({})
    assert behavior.is_silent(None, T0) is False
    assert behavior.provider_time(None, T0) == T0.isoformat()
    assert behavior.decide_offer(None, {"lease_id": "l"}) is ACCEPT
    plan = behavior.plan_job(None, {"lease_id": "l"}, 5)
    assert (plan.run_sec, plan.outcome, plan.reason) == (5, "SUCCESS", None)


# Provider wiring


def test_normal_provider_reports_success_after_duration():
    client = RecordingClient()
    provider = run(client, 6, params={"duration_sec": 5})
    # Accepted at t=1, finished at t=6
    assert client.reports == [("lease-1", "SUCCESS", None)]
    assert provider.events[0]["behavior"] == "normal"
    assert event_names(provider).count("reported") == 1


def test_provider_reports_what_plan_job_returns():
    class FailsFast(Behavior):
        def plan_job(self, provider, offer, duration_sec):
            return JobPlan(run_sec=2, outcome="FAILURE", reason="DISK_FULL")

    client = RecordingClient()
    run(client, 2, behavior=FailsFast({}), params={"duration_sec": 5})
    assert client.reports == []

    client = RecordingClient()
    run(client, 3, behavior=FailsFast({}), params={"duration_sec": 5})
    assert client.reports == [("lease-1", "FAILURE", "DISK_FULL")]


def test_provider_rejects_when_decide_offer_says_no():
    class Busy(Behavior):
        def decide_offer(self, provider, offer):
            return OfferDecision(False, "LOCAL_BUSY")

    client = RecordingClient()
    provider = run(client, 3, behavior=Busy({}))
    assert client.rejects == [("lease-1", "LOCAL_BUSY")]
    assert provider.lease_id is None
    assert "accepted" not in event_names(provider)


def test_provider_stops_when_silent():
    class Silent(Behavior):
        def is_silent(self, provider, now):
            return provider.heartbeat_count >= 2

    provider = run(RecordingClient(), 5, behavior=Silent({}))
    assert provider.heartbeat_count == 2
    assert provider.is_heartbeating is False
    assert event_names(provider).count("went_silent") == 1
