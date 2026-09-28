"""Execution-side failure modes. Names and signatures are placeholders for team design."""


def drain_mid_job(provider, lease):
    """Provider drains while running a job (reclaim needed)."""
    raise NotImplementedError("Team implementation pending")


def upload_fail(provider, lease):
    """Provider finishes the job but the artifact upload fails."""
    raise NotImplementedError("Team implementation pending")


def disk_full(provider, lease):
    """Provider runs out of disk during a job."""
    raise NotImplementedError("Team implementation pending")
