"""Unit tests for harness checks, using hand-made events and final_state."""

import asyncio
import json

from harness.checks import run_checks
from harness.checks.execution import check_disk_full, check_drain_mid_job, check_upload_fail
from harness.checks.invariants import check_invariants
from harness.checks.liveness import check_clock_skew, check_disappear
from harness.runner import collect_final_state, write_report


def event(provider, name, **details):
    return {"t": "2026-01-01T00:00:00+00:00", "provider": provider, "event": name, **details}


def all_passed(results):
    return all(r["status"] == "PASS" for r in results)


# ==========================================
# upload_fail checks
# ==========================================

UPLOAD_FAIL_EVENTS = [
    event("node-upload-fail", "registered", behavior="upload_fail"),
    event("node-upload-fail", "accepted", lease_id="lease-1", job_id="job-1"),
    event(
        "node-upload-fail",
        "reported",
        lease_id="lease-1",
        outcome="FAILURE",
        reason="ARTIFACT_UPLOAD_FAILED",
    ),
    event("node-upload-fail", "accepted", lease_id="lease-2", job_id="job-1"),
    event("node-upload-fail", "reported", lease_id="lease-2", outcome="SUCCESS"),
]


def upload_fail_state(attempt_count=2, job_status="COMPLETED", excluded_provider=None):
    rejected = [excluded_provider] if excluded_provider else []
    return {
        "status": {"providers": {"ACTIVE": 1}},
        "jobs": {
            "job-1": {
                "status": job_status,
                "attempt_count": attempt_count,
                "rejected_provider_ids": rejected,
                "current_lease_id": None,
            }
        },
        "leases": {
            "lease-1": {
                "job_id": "job-1",
                "status": "RELEASED",
                "outcome": "FAILURE",
                "reason": "ARTIFACT_UPLOAD_FAILED",
            },
            "lease-2": {
                "job_id": "job-1",
                "status": "RELEASED",
                "outcome": "SUCCESS",
                "reason": None,
            },
        },
    }


def test_upload_fail_passes_when_retried_and_completed():
    results = check_upload_fail(UPLOAD_FAIL_EVENTS, upload_fail_state())
    assert all_passed(results)


def test_upload_fail_fails_if_job_not_completed():
    results = check_upload_fail(UPLOAD_FAIL_EVENTS, upload_fail_state(job_status="FAILED"))
    failed = [r["check"] for r in results if r["status"] == "FAIL"]
    assert "all jobs COMPLETED" in failed


def test_upload_fail_fails_if_provider_was_wrongly_excluded():
    # Transient upload fail should NOT exclude the provider from retrying
    results = check_upload_fail(
        UPLOAD_FAIL_EVENTS, upload_fail_state(excluded_provider="node-upload-fail")
    )
    failed = [r["check"] for r in results if r["status"] == "FAIL"]
    assert "node-upload-fail not excluded from job-1" in failed


# ==========================================
# disk_full checks
# ==========================================

DISK_FULL_EVENTS = [
    event("node-disk-full", "registered", behavior="disk_full"),
    event("node-disk-full", "accepted", lease_id="lease-1", job_id="job-1"),
    event("node-disk-full", "reported", lease_id="lease-1", outcome="FAILURE", reason="DISK_FULL"),
]


def disk_full_state(
    rejected_providers=("node-disk-full",), job_status="COMPLETED", attempt_count=2
):
    return {
        "status": {"providers": {"ACTIVE": 2}},
        "jobs": {
            "job-1": {
                "status": job_status,
                "attempt_count": attempt_count,
                "rejected_provider_ids": list(rejected_providers),
                "current_lease_id": None,
            }
        },
        "leases": {
            "lease-1": {
                "job_id": "job-1",
                "status": "RELEASED",
                "outcome": "FAILURE",
                "reason": "DISK_FULL",
            },
        },
    }


def test_disk_full_passes_when_node_excluded_and_job_completes():
    results = check_disk_full(DISK_FULL_EVENTS, disk_full_state())
    assert all_passed(results)


def test_disk_full_fails_if_node_not_excluded():
    # If node-disk-full wasn't blacklisted for this job, check must fail
    results = check_disk_full(DISK_FULL_EVENTS, disk_full_state(rejected_providers=[]))
    failed = [r["check"] for r in results if r["status"] == "FAIL"]
    assert "node-disk-full excluded from retrying job-1" in failed


# ==========================================
# drain_mid_job checks
# ==========================================


def drain_events(finishes_at="2026-01-01T00:00:04+00:00", grace_period_seconds=6):
    return [
        event("node-drains", "registered", behavior="drain_mid_job"),
        event("node-drains", "accepted", lease_id="lease-1", job_id="job-1"),
        event(
            "node-drains",
            "drain_started",
            grace_period_seconds=grace_period_seconds,
            job_finishes_at=finishes_at,
        ),
    ]


def drain_state(reclaimed=False, provider_status="DRAINED"):
    leases = {
        "lease-1": {
            "job_id": "job-1",
            "provider_id": "node-drains",
            "status": "REVOKED" if reclaimed else "RELEASED",
            "outcome": None if reclaimed else "SUCCESS",
            "reason": "DRAIN_RECLAIM" if reclaimed else None,
        }
    }
    if reclaimed:
        leases["lease-2"] = {
            "job_id": "job-1",
            "provider_id": "node-normal",
            "status": "RELEASED",
            "outcome": "SUCCESS",
            "reason": None,
        }
    return {
        "status": {"providers": {provider_status: 1}},
        "providers": {"node-drains": {"status": provider_status}},
        "jobs": {
            "job-1": {
                "status": "COMPLETED",
                "attempt_count": 2 if reclaimed else 1,
                "current_lease_id": None,
            }
        },
        "leases": leases,
    }


def test_drain_meets_grace_passes():
    events = drain_events(finishes_at="2026-01-01T00:00:04+00:00", grace_period_seconds=6)
    assert all_passed(check_drain_mid_job(events, drain_state(reclaimed=False)))


def test_drain_meets_grace_fails_if_reclaimed():
    events = drain_events(finishes_at="2026-01-01T00:00:04+00:00", grace_period_seconds=6)
    assert not all_passed(check_drain_mid_job(events, drain_state(reclaimed=True)))


def test_drain_misses_grace_passes():
    events = drain_events(finishes_at="2026-01-01T00:00:10+00:00", grace_period_seconds=6)
    assert all_passed(check_drain_mid_job(events, drain_state(reclaimed=True)))


def test_drain_misses_grace_fails_if_not_reclaimed():
    events = drain_events(finishes_at="2026-01-01T00:00:10+00:00", grace_period_seconds=6)
    assert not all_passed(check_drain_mid_job(events, drain_state(reclaimed=False)))


def test_drain_fails_if_provider_not_drained():
    events = drain_events(finishes_at="2026-01-01T00:00:04+00:00", grace_period_seconds=6)
    state = drain_state(reclaimed=False, provider_status="ACTIVE")
    failed = [r["check"] for r in check_drain_mid_job(events, state) if r["status"] == "FAIL"]
    assert "node-drains DRAINED" in failed


# ==========================================
# invariants checks
# ==========================================


def test_invariants_pass_on_valid_state():
    state = {
        "jobs": {
            "job-1": {
                "status": "COMPLETED",
                "attempt_count": 1,
                "max_attempts": 3,
                "current_lease_id": None,
            }
        },
        "leases": {
            "lease-1": {
                "job_id": "job-1",
                "provider_id": "p1",
                "status": "RELEASED",
                "outcome": "SUCCESS",
            }
        },
    }
    assert all_passed(check_invariants([], state))


def test_invariants_fail_when_multiple_live_leases_on_same_job():
    state = {
        "jobs": {
            "job-1": {
                "status": "RUNNING",
                "attempt_count": 2,
                "max_attempts": 3,
                "current_lease_id": "lease-2",
            }
        },
        "leases": {
            "lease-1": {
                "job_id": "job-1",
                "provider_id": "p1",
                "status": "ACTIVE",
                "outcome": None,
            },
            "lease-2": {
                "job_id": "job-1",
                "provider_id": "p2",
                "status": "ACTIVE",
                "outcome": None,
            },
        },
    }
    results = check_invariants([], state)
    failed = [r["check"] for r in results if r["status"] == "FAIL"]
    assert "no job has multiple live leases simultaneously" in failed


def test_invariants_fail_when_attempts_exceed_max():
    state = {
        "jobs": {
            "job-1": {
                "status": "FAILED",
                "attempt_count": 4,
                "max_attempts": 3,
                "current_lease_id": None,
            }
        },
        "leases": {},
    }
    results = check_invariants([], state)
    failed = [r["check"] for r in results if r["status"] == "FAIL"]
    assert "no job exceeded max_attempts" in failed


# disappear
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
    assert report["summary"] == {"PASS": 1, "FAIL": 1, "ERROR": 0, "NOT_IMPLEMENTED": 1}
    assert report["results"] == results
    assert report["final_state"] == {"jobs": {}}
    assert report["events"] == events
