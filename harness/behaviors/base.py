"""Behavior base class: a normal provider, with one hook per lifecycle step.

FakeProvider owns the lifecycle and every HTTP call. At each step it asks its
Behavior what to do. A failure mode subclasses Behavior and overrides only the
hook for the step it changes, every other step stays normal.
"""


class OfferDecision:
    """Accept an offer, or reject it with a RejectReason value."""

    def __init__(self, accept, reason=None):
        self.accept = accept
        self.reason = reason


ACCEPT = OfferDecision(True)


class JobPlan:
    """What an accepted fake job will do, decided when it is accepted."""

    def __init__(self, run_sec, outcome="SUCCESS", reason=None):
        # Report this long after the job starts
        self.run_sec = run_sec
        # LeaseOutcome value
        self.outcome = outcome
        # FailureReason value when outcome is FAILURE
        self.reason = reason


class Behavior:
    """Normal provider. Failure modes subclass this and override one hook."""

    name = "normal"

    def __init__(self, params):
        self.params = params

    # Liveness

    def is_silent(self, provider, now):
        """True once the provider should stop all contact, as if it crashed."""
        return False

    def provider_time(self, provider, now):
        """The provider's own clock, sent with each heartbeat (logged only)."""
        return now.isoformat()

    # Lease

    def decide_offer(self, provider, offer):
        return ACCEPT

    def plan_job(self, provider, offer, duration_sec):
        return JobPlan(run_sec=duration_sec)

    # Execution

    async def on_running_tick(self, provider, now):
        """Called every tick while a job runs, after the normal lease step."""
