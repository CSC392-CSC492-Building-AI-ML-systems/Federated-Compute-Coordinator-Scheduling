"""Failure-mode logic for fake providers, grouped by the lifecycle step it changes.

Behaviors: normal, disappear, clock_skew, reject_lease, capability_lie,
drain_mid_job, upload_fail, disk_full.
"""

from harness.behaviors.base import ACCEPT, Behavior, JobPlan, OfferDecision, job_requirements
from harness.behaviors.lease import CapabilityLie, RejectLease
from harness.behaviors.liveness import ClockSkew, Disappear

BEHAVIORS = {cls.name: cls for cls in (Behavior, ClockSkew, Disappear, RejectLease, CapabilityLie)}

PLANNED = frozenset(
    {
        "drain_mid_job",
        "upload_fail",
        "disk_full",
    }
)


def make_behavior(name, params=None):
    """Build the Behavior for a scenario's `behavior` name. Fails fast on a typo."""
    if name in BEHAVIORS:
        return BEHAVIORS[name](params or {})
    if name in PLANNED:
        raise NotImplementedError(f"behavior {name!r} is planned but not implemented yet")
    raise ValueError(f"unknown behavior {name!r}; known: {sorted(BEHAVIORS)}")


__all__ = [
    "ACCEPT",
    "BEHAVIORS",
    "Behavior",
    "CapabilityLie",
    "ClockSkew",
    "Disappear",
    "JobPlan",
    "OfferDecision",
    "RejectLease",
    "job_requirements",
    "make_behavior",
]
