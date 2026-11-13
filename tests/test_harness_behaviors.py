import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from harness.behaviors import (
    ACCEPT,
    Behavior,
    CapabilityLie,
    ClockSkew,
    Disappear,
    DiskFull,
    DrainMidJob,
    JobPlan,
    OfferDecision,
    RejectLease,
    UploadFail,
    make_behavior,
)
from harness.provider import FakeProvider

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


class RecordingClient:
    """Fake coordinator: always ACTIVE, hands out one offer, records lease calls."""

    def __init__(self, required_vram_mb=8192):
        self.offers = [
            {
                "lease_id": "lease-1",
                "job_id": "job-1",
                "requirements": {
                    "required_vram_mb": required_vram_mb,
                    "gpu_models": [],
                    "required_runtime": "cuda12",
                    "tier": "standard",
                },
            }
        ]
        self.accepts = []
        self.rejects = []
        self.reports = []
        self.provider_times = []
        self.drains = []

    async def register_provider(self, data):
        return {"status": "ACTIVE"}

    async def heartbeat(self, provider_id, provider_time=None):
        self.provider_times.append(provider_time)
        return {"status": "ACTIVE"}

    async def drain(self, provider_id, grace_period_seconds):
        self.drains.append((provider_id, grace_period_seconds))
        return {"status": "DRAINING"}

    async def get_lease_offers(self, provider_id):
        if self.offers:
            return [self.offers.pop(0)]
        return []

    async def accept(self, lease_id, provider_id):
        self.accepts.append(lease_id)

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


def test_make_behavior_builds_disappear():
    behavior = make_behavior("disappear", {"disappear_after_heartbeats": 3})
    assert type(behavior) is Disappear
    assert behavior.after_heartbeats == 3


def test_disappear_goes_silent_correctly():
    client = RecordingClient()
    params = {"disappear_after_heartbeats": 3}
    provider = run(client, 10, behavior=Disappear(params), params=params)
    assert provider.heartbeat_count == 3
    assert provider.is_heartbeating is False
    # Goes silent permanently
    names = event_names(provider)
    assert names.count("went_silent") == 1
    assert "heartbeat" not in names[names.index("went_silent") :]


def test_disappear_keeps_lease():
    # Offer taken at t=1, silent at t=3 (after 2 heartbeats), job would finish at t=6
    client = RecordingClient()
    params = {"disappear_after_heartbeats": 2, "duration_sec": 5}
    provider = run(client, 10, behavior=Disappear(params), params=params)
    assert provider.lease_id == "lease-1"
    assert client.reports == []


def test_make_behavior_builds_clock_skew():
    behavior = make_behavior("clock_skew", {"skew_seconds": -10})
    assert type(behavior) is ClockSkew
    assert behavior.provider_time(None, T0) == (T0 - timedelta(seconds=10)).isoformat()


def test_clock_skew_sends_skewed_time_and_runs_normally():
    client = RecordingClient()
    params = {"skew_seconds": 3600, "duration_sec": 5}
    run(client, 6, behavior=ClockSkew(params), params=params)
    assert client.provider_times[0] == (T0 + timedelta(seconds=3601)).isoformat()
    assert len(client.provider_times) == 6
    assert client.reports == [("lease-1", "SUCCESS", None)]


# execution


def test_make_behavior_builds_execution_behaviors():
    b_upload = make_behavior("upload_fail", {"fail_times": 2})
    assert type(b_upload) is UploadFail
    assert b_upload.fail_times == 2

    b_disk = make_behavior("disk_full", {"fail_after_sec": 3})
    assert type(b_disk) is DiskFull
    assert b_disk.fail_after_sec == 3

    b_drain = make_behavior("drain_mid_job", {"drain_after_sec": 4, "grace_period_seconds": 8})
    assert type(b_drain) is DrainMidJob
    assert b_drain.drain_after_sec == 4
    assert b_drain.grace_period_seconds == 8


def test_upload_fail_fails_first_job_then_succeeds():
    client = RecordingClient()
    # Give it 2 offers: lease-1 then lease-2
    client.offers = [
        {"lease_id": "lease-1", "job_id": "job-1"},
        {"lease_id": "lease-2", "job_id": "job-2"},
    ]
    params = {"fail_times": 1, "duration_sec": 2}
    # Run for 6 seconds:
    # t=1 accept lease-1, t=3 finish lease-1 (FAILURE)
    # t=4 accept lease-2, t=6 finish lease-2 (SUCCESS)
    run(client, 6, behavior=UploadFail(params), params=params)

    assert client.reports == [
        ("lease-1", "FAILURE", "ARTIFACT_UPLOAD_FAILED"),
        ("lease-2", "SUCCESS", None),
    ]


def test_disk_full_fails_early_then_succeeds():
    client = RecordingClient()
    client.offers = [
        {"lease_id": "lease-1", "job_id": "job-1"},
        {"lease_id": "lease-2", "job_id": "job-2"},
    ]
    # fail_after_sec: 2 (even though duration_sec is 5)
    params = {"fail_times": 1, "fail_after_sec": 2, "duration_sec": 5}
    # t=1 accept lease-1, finishes early at t=3 (after 2s run_sec)
    run(client, 3, behavior=DiskFull(params), params=params)

    assert client.reports == [("lease-1", "FAILURE", "DISK_FULL")]


def test_drain_mid_job_triggers_drain_at_specified_time():
    client = RecordingClient()
    params = {"drain_after_sec": 2, "grace_period_seconds": 6, "duration_sec": 5}
    # Run for 3 seconds:
    # t=1: accepts job (job_started_at = T0 + 1s)
    # t=3: (now - job_started_at) is 2s >= drain_after_sec -> triggers drain
    provider = run(client, 3, behavior=DrainMidJob(params), params=params)

    assert client.drains == [("p1", 6)]
    assert provider.status == "DRAINING"
    assert "drain_started" in event_names(provider)


# reject_lease


def test_make_behavior_builds_reject_lease_with_default_reason():
    behavior = make_behavior("reject_lease", {})
    assert type(behavior) is RejectLease
    assert behavior.reason == "LOCAL_BUSY"


def test_reject_lease_rejects_unknown_reason_at_init():
    with pytest.raises(ValueError, match="BUSY_LOL"):
        RejectLease({"reject_reason": "BUSY_LOL"})


def test_reject_lease_decides_reject_with_reason():
    decision = RejectLease({"reject_reason": "TEMP_UNAVAILABLE"}).decide_offer(None, {})
    assert (decision.accept, decision.reason) == (False, "TEMP_UNAVAILABLE")


def test_reject_lease_provider_rejects_and_never_runs_a_job():
    client = RecordingClient()
    params = {"reject_reason": "LOCAL_BUSY"}
    provider = run(client, 8, behavior=RejectLease(params), params=params)
    assert client.rejects == [("lease-1", "LOCAL_BUSY")]
    assert client.accepts == []
    assert client.reports == []
    rejected = [e for e in provider.events if e["event"] == "rejected"]
    assert rejected[0]["job_id"] == "job-1"
    assert rejected[0]["reason"] == "LOCAL_BUSY"
    # Rejecting is not a failure: it keeps heartbeating and stays available
    assert provider.is_heartbeating is True


# capability_lie


def test_make_behavior_builds_capability_lie():
    behavior = make_behavior("capability_lie", {"actual_vram_mb": 6144})
    assert type(behavior) is CapabilityLie
    assert (behavior.actual_vram_mb, behavior.fail_after_sec) == (6144, 1)


def test_capability_lie_needs_actual_vram():
    with pytest.raises(ValueError, match="actual_vram_mb"):
        CapabilityLie({})


def offer_needing(vram_mb):
    return {"lease_id": "l", "job_id": "j", "requirements": {"required_vram_mb": vram_mb}}


def test_capability_lie_fails_job_that_needs_more_than_real_vram():
    plan = CapabilityLie({"actual_vram_mb": 6144}).plan_job(None, offer_needing(16384), 5)
    assert (plan.run_sec, plan.outcome, plan.reason) == (1, "FAILURE", "CAPABILITY_MISMATCH")


def test_capability_lie_runs_job_that_fits_real_vram_normally():
    behavior = CapabilityLie({"actual_vram_mb": 6144})
    for vram_mb in (4096, 6144):
        plan = behavior.plan_job(None, offer_needing(vram_mb), 5)
        assert (plan.run_sec, plan.outcome, plan.reason) == (5, "SUCCESS", None)


def test_capability_lie_honours_fail_after_sec():
    plan = CapabilityLie({"actual_vram_mb": 6144, "fail_after_sec": 3}).plan_job(
        None, offer_needing(16384), 5
    )
    assert plan.run_sec == 3


def test_capability_lie_provider_accepts_then_reports_mismatch_after_1s():
    # Accepted at t=1, fails at t=2 instead of finishing at t=6
    client = RecordingClient(required_vram_mb=16384)
    params = {"actual_vram_mb": 6144, "duration_sec": 5}
    provider = run(client, 2, behavior=CapabilityLie(params), params=params)
    assert client.accepts == ["lease-1"]
    assert client.reports == [("lease-1", "FAILURE", "CAPABILITY_MISMATCH")]
    accepted = next(e for e in provider.events if e["event"] == "accepted")
    assert accepted["planned_outcome"] == "FAILURE"
    # Free again for the next offer
    assert provider.lease_id is None
