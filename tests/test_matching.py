"""Tests for the job-to-provider matching policy."""

from datetime import datetime, timezone

import pytest

from coordinator.models.job import Job, JobStatus
from coordinator.models.provider import Capabilities, Provider, ProviderStatus
from coordinator.policies.matching import can_run

NOW = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def base_provider():
    return Provider(
        provider_id="provider-1",
        status=ProviderStatus.ACTIVE,
        capabilities=Capabilities(
            gpu_model="A100", vram_mb=40960, driver_version="550.54", runtimes=["cuda12", "docker"]
        ),
        accepted_tiers=["tier1", "tier2"],
        registered_at=NOW,
        last_heartbeat_at=NOW,
    )


@pytest.fixture
def base_job():
    return Job(
        job_id="job-1",
        status=JobStatus.QUEUED,
        required_vram_mb=24000,
        gpu_models=["A100", "H100"],
        required_runtime="cuda12",
        tier="tier1",
        created_at=NOW,
    )


def test_can_run_returns_true_for_matching_pair(base_provider, base_job):
    assert can_run(base_provider, base_job) is True


def test_fails_if_provider_not_active(base_provider, base_job):
    base_provider.status = ProviderStatus.DRAINING
    assert can_run(base_provider, base_job) is False


def test_fails_if_job_not_queued(base_provider, base_job):
    base_job.status = JobStatus.RUNNING
    assert can_run(base_provider, base_job) is False


def test_fails_if_job_has_active_lease(base_provider, base_job):
    base_job.current_lease_id = "lease-123"
    assert can_run(base_provider, base_job) is False


def test_fails_if_provider_in_rejected_list(base_provider, base_job):
    base_job.rejected_provider_ids.add("provider-1")
    assert can_run(base_provider, base_job) is False


def test_fails_if_vram_too_low(base_provider, base_job):
    base_provider.capabilities.vram_mb = 16384
    assert can_run(base_provider, base_job) is False


def test_fails_if_runtime_missing(base_provider, base_job):
    base_job.required_runtime = "tensorrt"
    assert can_run(base_provider, base_job) is False


def test_fails_if_tier_not_accepted(base_provider, base_job):
    base_job.tier = "tier3"
    assert can_run(base_provider, base_job) is False


def test_fails_if_gpu_model_not_in_list(base_provider, base_job):
    base_job.gpu_models = ["RTX4090"]
    assert can_run(base_provider, base_job) is False


def test_passes_if_job_allows_any_gpu_model(base_provider, base_job):
    base_job.gpu_models = []
    assert can_run(base_provider, base_job) is True
