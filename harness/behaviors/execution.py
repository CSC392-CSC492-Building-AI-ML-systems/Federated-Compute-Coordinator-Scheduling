"""Execution-side failure modes. Names and signatures are placeholders for team design."""

from harness.behaviors.base import Behavior, JobPlan


class UploadFail(Behavior):
    """Provider running full job but sends fail signal
    due to being unable to transfer, network and other
    wide range of possible issues."""

    name = "upload_fail"

    def __init__(self, params):
        super().__init__(params)
        self.fail_times = params.get("fail_times", 1)
        self.failed_count = 0

    def plan_job(self, provider, offer, duration_sec):
        if self.failed_count < self.fail_times:
            self.failed_count += 1
            return JobPlan(run_sec=duration_sec, outcome="FAILURE", reason="ARTIFACT_UPLOAD_FAILED")
        return JobPlan(run_sec=duration_sec, outcome="SUCCESS")


class DiskFull(Behavior):
    name = "disk_full"

    def __init__(self, params):
        super().__init__(params)
        self.fail_times = params.get("fail_times", 1)
        self.fail_after_sec = params.get("fail_after_sec", 2)
        self.failed_count = 0

    def plan_job(self, provider, offer, duration_sec):
        if self.failed_count < self.fail_times:
            self.failed_count += 1
            return JobPlan(run_sec=self.fail_after_sec, outcome="FAILURE", reason="DISK_FULL")
        return JobPlan(run_sec=duration_sec, outcome="SUCCESS")


class DrainMidJob(Behavior):
    name = "drain_mid_job"

    def __init__(self, params):
        super().__init__(params)
        self.drain_after_sec = params.get("drain_after_sec", 2)
        self.grace_period_seconds = params.get("grace_period_seconds", 5)
        self.drained = False

    # Behaviour to be discussed
    async def on_running_tick(self, provider, now):
        if self.drained or provider.job_started_at is None:
            return
        if (now - provider.job_started_at).total_seconds() >= self.drain_after_sec:
            self.drained = True
            await provider.drain(now, self.grace_period_seconds)
