"""Expected reactions to execution-side failure modes."""


def check_drain_mid_job(events, final_state):
    """Within grace the job completes; past grace the lease is reclaimed and retried."""
    raise NotImplementedError("Team implementation pending")


def check_upload_fail(events, final_state):
    """Lease reports ARTIFACT_UPLOAD_FAILED; retry check runs."""
    raise NotImplementedError("Team implementation pending")


def check_disk_full(events, final_state):
    """Lease reports DISK_FULL; retry check runs."""
    raise NotImplementedError("Team implementation pending")
