"""Decide whether node failures justify UNHEALTHY. Rules are not implemented yet."""


def should_mark_unhealthy(provider):
    # Policies return decisions; they do not change stored state.
    raise NotImplementedError("Team implementation pending")
