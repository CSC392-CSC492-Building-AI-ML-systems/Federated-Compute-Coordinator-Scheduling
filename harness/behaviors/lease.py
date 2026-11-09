"""Offer side failure modes: what a provider does with a lease offer and the job in it."""

from harness.behaviors.base import Behavior, JobPlan, OfferDecision, job_requirements

REJECT_REASONS = frozenset(
    {
        "LOCAL_BUSY",
        "TEMP_UNAVAILABLE",
        "POLICY_REJECT",
        "CAPABILITY_MISMATCH",
        "RESOURCE_UNAVAILABLE",
    }
)


class RejectLease(Behavior):
    """Rejects every offer with `reject_reason`, as if busy with local work.
    The coordinator should mark the lease REJECTED, keep the job QUEUED without
    using an attempt, and not offer that job to this provider again."""

    name = "reject_lease"

    def __init__(self, params):
        super().__init__(params)
        self.reason = params.get("reject_reason", "LOCAL_BUSY")
        if self.reason not in REJECT_REASONS:
            raise ValueError(
                f"reject_reason {self.reason!r} is not a RejectReason; "
                f"known: {sorted(REJECT_REASONS)}"
            )

    def decide_offer(self, provider, offer):
        return OfferDecision(False, self.reason)


class CapabilityLie(Behavior):
    """Advertises more VRAM than it has (`actual_vram_mb`). It accepts any job the
    coordinator offers, and a job that needs more than the real VRAM fails with
    CAPABILITY_MISMATCH after `fail_after_sec`, like an out-of-memory crash. Jobs
    that fit the real VRAM run normally. The coordinator should retry the failed
    job on another provider."""

    name = "capability_lie"

    def __init__(self, params):
        super().__init__(params)
        if "actual_vram_mb" not in params:
            raise ValueError("capability_lie needs actual_vram_mb (the real VRAM)")
        self.actual_vram_mb = params["actual_vram_mb"]
        self.fail_after_sec = params.get("fail_after_sec", 1)

    def plan_job(self, provider, offer, duration_sec):
        if job_requirements(offer)["required_vram_mb"] > self.actual_vram_mb:
            return JobPlan(self.fail_after_sec, "FAILURE", "CAPABILITY_MISMATCH")
        return JobPlan(run_sec=duration_sec)
