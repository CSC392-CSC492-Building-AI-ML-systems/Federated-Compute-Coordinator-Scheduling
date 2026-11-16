"""Unit tests for harness checks, using hand-made events and final_state."""

import asyncio
import json

from harness.checks import run_checks
from harness.checks.liveness import check_clock_skew, check_disappear
from harness.runner import collect_final_state, write_report


def event(provider, name, **details):
    return {"t": "2026-01-01T00:00:00+00:00", "provider": provider, "event": name, **details}


def all_passed(results):
    return all(r["status"] == "PASS" for r in results)


# disappear

DISAPPEAR_EVENTS = [
    event("node-normal", "registered", behavior="normal"),
    event("node-vanishes", "registered", behavior="disappear"),
    event("node-vanishes", "accepted", lease_id="lease-1", job_id="job-1"),
    event("node-normal", "accepted", lease_id="lease-2", job_id="job-2"),
    event("node-vanishes", "went_silent", heartbeat_count=2),
    event("node-normal", "reported", lease_id="lease-2", outcome="SUCCESS"),
    event("node-normal", "accepted", lease_id="lease-3", job_id="job-1"),
    event("node-normal", "reported", lease_id="lease-3", outcome="SUCCESS"),
]


def disappear_state(vanished_status="STALE", lease_1_reason="PROVIDER_STALE"):
    return {
        "status": {"providers": {"ACTIVE": 1, vanished_status: 1}},
        "jobs": {
            "job-1": {"status": "COMPLETED", "attempt_count": 2},
            "job-2": {"status": "COMPLETED", "attempt_count": 1},
        },
        "leases": {
            "lease-1": {"job_id": "job-1", "status": "REVOKED", "reason": lease_1_reason},
            "lease-2": {"job_id": "job-2", "status": "RELEASED", "reason": None},
            "lease-3": {"job_id": "job-1", "status": "RELEASED", "reason": None},
        },
    }


def test_disappear_passes_when_coordinator_reacts_correctly():
    results = check_disappear(DISAPPEAR_EVENTS, disappear_state())
    assert all_passed(results)
    assert len(results) == 3


def test_disappear_fails_if_provider_never_goes_stale():
    results = check_disappear(DISAPPEAR_EVENTS, disappear_state(vanished_status="ACTIVE"))
    failed = [r["check"] for r in results if r["status"] == "FAIL"]
    assert failed == ["1 provider(s) STALE"]


def test_disappear_fails_if_lease_revoked_for_wrong_reason():
    state = disappear_state(lease_1_reason="EXECUTION_TIMEOUT")
    failed = [r["check"] for r in check_disappear(DISAPPEAR_EVENTS, state) if r["status"] == "FAIL"]
    assert failed == ["lease-1 revoked for PROVIDER_STALE"]


def test_disappear_fails_if_no_provider_used_it():
    results = check_disappear([event("node-normal", "registered", behavior="normal")], {})
    assert not all_passed(results)


def test_disappear_does_not_raise_on_missing_records():
    results = check_disappear(DISAPPEAR_EVENTS, {})
    assert not all_passed(results)


# clock_skew

SKEW_EVENTS = [
    event("node-normal", "registered", behavior="normal"),
    event("node-skew-ahead", "registered", behavior="clock_skew"),
    event("node-skew-behind", "registered", behavior="clock_skew"),
]


def skew_state(behind_status="ACTIVE", job_2_attempts=1, lease_reason=None):
    counts = {"ACTIVE": 2}
    counts[behind_status] = counts.get(behind_status, 0) + 1
    return {
        "status": {"providers": counts},
        "jobs": {
            "job-1": {"status": "COMPLETED", "attempt_count": 1},
            "job-2": {"status": "COMPLETED", "attempt_count": job_2_attempts},
        },
        "leases": {
            "lease-1": {"job_id": "job-1", "status": "RELEASED", "reason": None},
            "lease-2": {"job_id": "job-2", "status": "RELEASED", "reason": lease_reason},
        },
    }


def test_clock_skew_passes_when_coordinator_ignores_provider_time():
    assert all_passed(check_clock_skew(SKEW_EVENTS, skew_state()))


def test_clock_skew_fails_if_skewed_provider_goes_stale():
    results = check_clock_skew(SKEW_EVENTS, skew_state(behind_status="STALE"))
    failed = [r["check"] for r in results if r["status"] == "FAIL"]
    assert failed == ["all 3 providers ACTIVE"]


def test_clock_skew_fails_if_a_job_needed_a_retry():
    state = skew_state(job_2_attempts=2, lease_reason="PROVIDER_STALE")
    failed = [r["check"] for r in check_clock_skew(SKEW_EVENTS, state) if r["status"] == "FAIL"]
    assert failed == ["no lease revoked for PROVIDER_STALE", "job-2 COMPLETED on first attempt"]


def test_clock_skew_fails_if_no_provider_used_it():
    results = check_clock_skew([event("node-normal", "registered", behavior="normal")], {})
    assert not all_passed(results)


# runner plumbing


class StateClient:
    """Answers the read calls collect_final_state makes."""

    def __init__(self):
        self.jobs = {
            "job-1": {"job_id": "job-1", "status": "QUEUED", "current_lease_id": "lease-9"},
            "job-2": {"job_id": "job-2", "status": "COMPLETED", "current_lease_id": None},
        }

    async def get_job(self, job_id):
        return self.jobs[job_id]

    async def get_lease(self, lease_id):
        return {"lease_id": lease_id}

    async def get_status(self):
        return {"providers": {"ACTIVE": 2}, "jobs": {}, "leases": {}}


def test_collect_final_state_fetches_jobs_status_and_every_known_lease():
    events = [
        event("p1", "registered", behavior="normal"),
        event("p1", "accepted", lease_id="lease-1", job_id="job-2"),
        event("p1", "reported", lease_id="lease-1", outcome="SUCCESS"),
        event("p2", "rejected", lease_id="lease-2", job_id="job-1"),
    ]
    state = asyncio.run(collect_final_state(StateClient(), events, ["job-1", "job-2"]))
    assert set(state["jobs"]) == {"job-1", "job-2"}
    # lease-1 and lease-2 from events, lease-9 from job-1's current lease
    assert set(state["leases"]) == {"lease-1", "lease-2", "lease-9"}
    assert state["status"]["providers"] == {"ACTIVE": 2}


def test_run_checks_runs_the_check_of_each_behavior_in_the_run():
    results = run_checks(SKEW_EVENTS, skew_state())
    checks = [r["check"] for r in results]
    assert "all 3 providers ACTIVE" in checks
    # disappear was not in this run, so its check does not run
    assert "disappear provider in run" not in checks


def test_run_checks_skips_checks_that_are_not_written_yet():
    events = [event("p1", "registered", behavior="reject_lease")]
    results = run_checks(events, {})
    assert {"check": "reject_lease check", "status": "NOT_IMPLEMENTED", "detail": ""} in results


def test_write_report_saves_the_run_next_to_the_scenario(tmp_path):
    scenario = tmp_path / "disappear.yaml"
    results = [
        {"check": "a", "status": "PASS", "detail": ""},
        {"check": "b", "status": "FAIL", "detail": ""},
        {"check": "c", "status": "NOT_IMPLEMENTED", "detail": ""},
    ]
    events = [event("p1", "registered", behavior="disappear")]
    path = write_report(scenario, events, {"jobs": {}}, results)

    assert path == tmp_path / "disappear_report.json"
    report = json.loads(path.read_text())
    assert report["scenario"] == "disappear"
    assert report["summary"] == {"PASS": 1, "FAIL": 1, "NOT_IMPLEMENTED": 1}
    assert report["results"] == results
    assert report["final_state"] == {"jobs": {}}
    assert report["events"] == events
