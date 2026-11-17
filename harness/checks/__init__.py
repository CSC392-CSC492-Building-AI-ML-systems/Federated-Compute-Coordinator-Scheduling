"""Expected coordinator reactions, checked after a scenario run.

Checks return results; they never raise on failure, so a run always finishes
with a full report.
"""

from harness.checks.execution import check_disk_full, check_drain_mid_job, check_upload_fail
from harness.checks.lease import check_capability_lie, check_reject_lease
from harness.checks.liveness import check_clock_skew, check_disappear

# Behavior name -> its check, mirrors harness.behaviors.BEHAVIORS.
# "normal" has no check of its own.
CHECKS = {
    "disappear": check_disappear,
    "clock_skew": check_clock_skew,
    "reject_lease": check_reject_lease,
    "capability_lie": check_capability_lie,
    "drain_mid_job": check_drain_mid_job,
    "upload_fail": check_upload_fail,
    "disk_full": check_disk_full,
}


def run_checks(events, final_state):
    """Run the check of every behavior used in the run.
    A check that is not written yet gets status NOT_IMPLEMENTED, not PASS or FAIL."""
    behaviors = sorted(
        {e["behavior"] for e in events if e["event"] == "registered" and e["behavior"] in CHECKS}
    )
    # TODO: add in the invariant checks (check_invariants in checks/invariants.py)
    to_run = [(b, CHECKS[b]) for b in behaviors]

    results = []
    for name, check in to_run:
        try:
            results.extend(check(events, final_state))
        except NotImplementedError:
            results.append({"check": f"{name} check", "status": "NOT_IMPLEMENTED", "detail": ""})
    return results


__all__ = ["CHECKS", "run_checks"]
