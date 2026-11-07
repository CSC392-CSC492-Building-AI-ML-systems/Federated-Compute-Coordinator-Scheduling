"""Execution-side failure modes. Names and signatures are placeholders for team design."""

from harness.behaviors.base import Behavior


class DrainMidJob(Behavior):
    """Provider starts a drain while a job is still running.
    meets_grace scenarios give a grace longer than the remaining job time,
    so the job finishes, the others give a shorter one, so the coordinator
    must reclaim the lease at the drain deadline."""

    name = "drain_mid_job"

    def __init__(self, params):
        super().__init__(params)
        self.drain_after_sec = params.get("drain_after_sec", 2)
        self.grace_period_seconds = params.get("grace_period_seconds", 6)

    async def on_running_tick(self, provider, now):
        if provider.status != "ACTIVE":
            return
        if (now - provider.job_started_at).total_seconds() >= self.drain_after_sec:
            await provider.drain(now, self.grace_period_seconds)


def upload_fail(provider, lease):
    """Provider finishes the job but the artifact upload fails."""
    raise NotImplementedError("Team implementation pending")


def disk_full(provider, lease):
    """Provider runs out of disk during a job."""
    raise NotImplementedError("Team implementation pending")
