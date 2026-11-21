from harness.checks.execution import check_disk_full, check_drain_mid_job, check_upload_fail
from harness.checks.invariants import check_invariants


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

DRAIN_EVENTS = [
    event("node-drains", "registered", behavior="drain_mid_job"),
    event("node-drains", "accepted", lease_id="lease-1", job_id="job-1"),
    event("node-drains", "drain_started", grace_period_seconds=6),
]


def drain_state(reclaimed=False):
    reason = "DRAIN_RECLAIM" if reclaimed else None
    outcome = None if reclaimed else "SUCCESS"
    return {
        "status": {"providers": {"DRAINED": 1}},
        "jobs": {
            "job-1": {
                "status": "COMPLETED",
                "attempt_count": 2 if reclaimed else 1,
                "current_lease_id": None,
            }
        },
        "leases": {
            "lease-1": {
                "job_id": "job-1",
                "status": "REVOKED" if reclaimed else "RELEASED",
                "outcome": outcome,
                "reason": reason,
            }
        },
    }


def test_drain_meets_grace_passes():
    results = check_drain_mid_job(DRAIN_EVENTS, drain_state(reclaimed=False))
    assert all_passed(results)


def test_drain_misses_grace_passes():
    results = check_drain_mid_job(DRAIN_EVENTS, drain_state(reclaimed=True))
    assert all_passed(results)


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
