"""Heartbeat-side failure modes. Names and signatures are placeholders for team design."""

from datetime import timedelta

from harness.behaviors.base import Behavior


class Disappear(Behavior):
    """Provider stops heartbeating and goes silent (node disappears).
    Keeps the lease it holds, the coordinator must notice, revoke the
    lease, then requeue the job"""

    name = "disappear"

    def __init__(self, params):
        super().__init__(params)
        # heartbeats sent before stopping the heartbeat (using the shared.yaml value)
        self.after_heartbeats = params.get("disappear_after_heartbeats", 2)

    # causes the went_silent log event
    def is_silent(self, provider, now):
        return provider.heartbeat_count >= self.after_heartbeats


class ClockSkew(Behavior):
    """Gives the provider's clock an offset compared to the coordinator's. Only the provider_time
    sent with each heartbeat changes, the coordinator should ignore it and keep
    using its own clock"""

    name = "clock_skew"

    def __init__(self, params):
        super().__init__(params)
        # Seconds the provider's clock is ahead if +ve, -ve if behind
        self.skew = timedelta(seconds=params.get("skew_seconds", 3600))

    def provider_time(self, provider, now):
        return (now + self.skew).isoformat()
