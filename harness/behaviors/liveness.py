"""Heartbeat-side failure modes. Names and signatures are placeholders for team design."""


def disappear(provider):
    """Provider stops heartbeating and goes silent (node disappears)."""
    raise NotImplementedError("Team implementation pending")


def clock_skew(provider):
    """Provider's clock is offset from the coordinator's."""
    raise NotImplementedError("Team implementation pending")
