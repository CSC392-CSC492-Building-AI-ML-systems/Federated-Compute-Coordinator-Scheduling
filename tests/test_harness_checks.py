"""Unit tests for harness checks, using hand-made events and final_state."""

import asyncio
import json

import pytest

from harness.checks import CHECKS, run_checks
from harness.checks.lease import check_capability_lie, check_reject_lease
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
        "providers": {
            "node-normal": {"status": "ACTIVE"},
            "node-vanishes": {"status": vanished_status},
        },
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
    assert failed == ["node-vanishes STALE"]


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
    return {
        "providers": {
            "node-normal": {"status": "ACTIVE"},
            "node-skew-ahead": {"status": "ACTIVE"},
            "node-skew-behind": {"status": behind_status},
        },
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
    assert failed == ["node-skew-behind ACTIVE"]


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

    async def get_provider(self, provider_id):
        return {"provider_id": provider_id, "status": "ACTIVE"}

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
    # only p1 registered, so only p1 is fetched
    assert set(state["providers"]) == {"p1"}


def test_run_checks_runs_the_check_of_each_behavior_in_the_run():
    results = run_checks(SKEW_EVENTS, skew_state())
    checks = [r["check"] for r in results]
    assert "node-skew-behind ACTIVE" in checks
    # disappear was not in this run, so its check does not run
    assert "disappear provider in run" not in checks


def test_run_checks_skips_checks_that_are_not_written_yet(monkeypatch):
    # A stand-in, so the test does not depend on which real checks are still stubs
    def not_written(events, final_state):
        raise NotImplementedError

    monkeypatch.setitem(CHECKS, "disk_full", not_written)
    events = [event("p1", "registered", behavior="disk_full")]
    results = run_checks(events, {})
    assert {"check": "disk_full check", "status": "NOT_IMPLEMENTED", "detail": ""} in results


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


# lease

# One rejection: node-rejector rejects job-2, node-normal then finishes it.
REJECT_EVENTS = [
    event("node-normal", "registered", behavior="normal"),
    event("node-rejector", "registered", behavior="reject_lease"),
    event("node-rejector", "rejected", lease_id="lease-1", job_id="job-1", reason="LOCAL_BUSY"),
    event("node-normal", "accepted", lease_id="lease-2", job_id="job-1", planned_outcome="SUCCESS"),
    event("node-normal", "reported", lease_id="lease-2", outcome="SUCCESS", reason=None),
]


def reject_state(lease_status="REJECTED", excluded=True, attempts=1, job_status="COMPLETED"):
    return {
        "providers": {"node-normal": {"status": "ACTIVE"}, "node-rejector": {"status": "ACTIVE"}},
        "jobs": {
            "job-1": {
                "status": job_status,
                "attempt_count": attempts,
                "rejected_provider_ids": ["node-rejector"] if excluded else [],
            }
        },
        "leases": {
            "lease-1": {"job_id": "job-1", "status": lease_status, "reason": "LOCAL_BUSY"},
            "lease-2": {"job_id": "job-1", "status": "RELEASED", "outcome": "SUCCESS"},
        },
    }


# One mismatch: node-liar fails job-1 with CAPABILITY_MISMATCH, node-normal retries it.
LIE_EVENTS = [
    event("node-normal", "registered", behavior="normal"),
    event("node-liar", "registered", behavior="capability_lie"),
    event("node-liar", "accepted", lease_id="lease-1", job_id="job-1", planned_outcome="FAILURE"),
    event(
        "node-liar", "reported", lease_id="lease-1", outcome="FAILURE", reason="CAPABILITY_MISMATCH"
    ),
    event("node-normal", "accepted", lease_id="lease-2", job_id="job-1", planned_outcome="SUCCESS"),
    event("node-normal", "reported", lease_id="lease-2", outcome="SUCCESS", reason=None),
]


def lie_state(lease_reason="CAPABILITY_MISMATCH", excluded=True, job_status="COMPLETED"):
    return {
        "providers": {"node-normal": {"status": "ACTIVE"}, "node-liar": {"status": "ACTIVE"}},
        "jobs": {
            "job-1": {
                "status": job_status,
                "attempt_count": 2,
                "rejected_provider_ids": ["node-liar"] if excluded else [],
                "last_failure_reason": "CAPABILITY_MISMATCH",
            }
        },
        "leases": {
            "lease-1": {
                "job_id": "job-1",
                "status": "RELEASED",
                "outcome": "FAILURE",
                "reason": lease_reason,
            },
            "lease-2": {"job_id": "job-1", "status": "RELEASED", "outcome": "SUCCESS"},
        },
    }


def test_reject_lease_passes_when_coordinator_follows_the_contract():
    assert all_passed(check_reject_lease(REJECT_EVENTS, reject_state()))


@pytest.mark.parametrize(
    "mistake",
    [
        {"lease_status": "EXPIRED"},  # reject not recorded
        {"excluded": False},  # rejector not excluded from the job
        {"attempts": 2},  # rejection used an attempt
        {"job_status": "QUEUED"},  # job never finished elsewhere
    ],
)
def test_reject_lease_fails_on_coordinator_mistake(mistake):
    assert not all_passed(check_reject_lease(REJECT_EVENTS, reject_state(**mistake)))


def test_reject_lease_fails_if_job_offered_to_rejector_again():
    events = REJECT_EVENTS + [
        event("node-rejector", "rejected", lease_id="lease-3", job_id="job-1", reason="LOCAL_BUSY")
    ]
    state = reject_state()
    state["leases"]["lease-3"] = {"job_id": "job-1", "status": "REJECTED", "reason": "LOCAL_BUSY"}
    assert not all_passed(check_reject_lease(events, state))


def test_capability_lie_passes_when_coordinator_follows_the_contract():
    assert all_passed(check_capability_lie(LIE_EVENTS, lie_state()))


@pytest.mark.parametrize(
    "events, mistake",
    [
        (LIE_EVENTS, {"lease_reason": "RUNTIME_ERROR"}),  # mismatch not recorded
        (LIE_EVENTS, {"excluded": False}),  # liar not excluded from the job
        (LIE_EVENTS[:-2], {"job_status": "QUEUED"}),  # job never retried elsewhere
    ],
)
def test_capability_lie_fails_on_coordinator_mistake(events, mistake):
    assert not all_passed(check_capability_lie(events, lie_state(**mistake)))


def test_capability_lie_allows_a_later_failure_on_another_node():
    # After the mismatch, job-1 fails again elsewhere before it completes. Its
    # last_failure_reason is then DISK_FULL, which is still correct.
    events = LIE_EVENTS[:-2] + [
        event("node-flaky", "accepted", lease_id="lease-2", job_id="job-1"),
        event("node-flaky", "reported", lease_id="lease-2", outcome="FAILURE", reason="DISK_FULL"),
        event("node-normal", "accepted", lease_id="lease-3", job_id="job-1"),
        event("node-normal", "reported", lease_id="lease-3", outcome="SUCCESS", reason=None),
    ]
    state = lie_state()
    state["jobs"]["job-1"].update(attempt_count=3, last_failure_reason="DISK_FULL")
    assert all_passed(check_capability_lie(events, state))


LEASE_CHECKS = [
    (check_reject_lease, REJECT_EVENTS, reject_state, "rejected"),
    (check_capability_lie, LIE_EVENTS, lie_state, "reported"),
]


@pytest.mark.parametrize("check, events, state, _", LEASE_CHECKS)
def test_lease_check_fails_if_final_state_not_collected(check, events, state, _):
    assert not all_passed(check(events, {"error": "ConnectError: refused"}))


@pytest.mark.parametrize("check, events, state, faulty_event", LEASE_CHECKS)
def test_lease_check_fails_if_faulty_behavior_never_fired(check, events, state, faulty_event):
    # The faulty provider is always eligible, so no rejection / mismatch report means
    # the coordinator never offered it a job or refused its call
    quiet = [e for e in events if e["event"] != faulty_event or e["provider"] == "node-normal"]
    assert not all_passed(check(quiet, state()))
