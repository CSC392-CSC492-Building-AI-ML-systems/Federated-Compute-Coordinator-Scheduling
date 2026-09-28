"""Decide whether a failed attempt should be retried. Rules are not implemented yet."""


def should_retry(job, failure):
    # Policies return decisions; they do not change stored state.
    raise NotImplementedError("Team implementation pending")
