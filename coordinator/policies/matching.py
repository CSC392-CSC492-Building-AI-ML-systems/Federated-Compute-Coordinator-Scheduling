"""Decide whether a provider can run a job. Rules are not implemented yet."""

from coordinator.models.job import JobStatus


def can_run(provider, job):
    """
    Verifies whether the provider is capable of
    executing the given job
    """

    # If the provider is not ACTIVE or if the job is not QUEUED, return False
    if provider.status != "ACTIVE" or job.status != JobStatus.QUEUED:
        return False

    # Job cannot have an active lease (One job per lease)
    if job.current_lease_id is not None:
        return False

    # Provider must not be in the job's rejected list
    if provider.id in job.rejected_provider_ids:
        return False

    # Provider VRAM must meet or exceed the requirement
    if provider.vram_mb < job.required_vram_mb:
        return False

    # Job's required runtime must be supported by the provider
    if job.required_runtime not in provider.runtimes:
        return False

    # 7. Job's tier must be accepted by the provider
    if job.tier not in provider.accepted_tiers:
        return False

    # GPU model match: Job allows any (empty) OR provider's model is in the list
    if job.gpu_models and provider.gpu_model not in job.gpu_models:
        return False

    return True
