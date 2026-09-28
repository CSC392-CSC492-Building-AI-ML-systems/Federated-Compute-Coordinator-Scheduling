"""Expected reactions to heartbeat-side failure modes."""


def check_disappear(events, final_state):
    """Provider becomes STALE; its leases are revoked; jobs retry elsewhere."""
    raise NotImplementedError("Team implementation pending")


def check_clock_skew(events, final_state):
    """Skewed provider time does not affect stale detection."""
    raise NotImplementedError("Team implementation pending")
