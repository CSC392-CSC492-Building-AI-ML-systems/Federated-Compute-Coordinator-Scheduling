"""Expected reactions to offer-side failure modes."""


def check_reject_lease(events, final_state):
    """Lease is REJECTED without using an attempt; job is offered elsewhere."""
    raise NotImplementedError("Team implementation pending")


def check_capability_lie(events, final_state):
    """Lease reports CAPABILITY_MISMATCH; job retries on another provider."""
    raise NotImplementedError("Team implementation pending")
