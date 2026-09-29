"""Schema tests for the Provider, Job, and Lease models. No services or HTTP involved."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from coordinator.models.job import Job, JobStatus, JobSubmitRequest
from coordinator.models.lease import (
    TERMINAL_LEASE_STATUSES,
    Lease,
    LeaseActionRequest,
    LeaseOutcome,
    LeaseStatus,
    RejectRequest,
    RenewRequest,
    ReportRequest,
)
from coordinator.models.provider import (
    Capabilities,
    DrainRequest,
    Provider,
    ProviderRegisterRequest,
    ProviderStatus,
)

# A fixed time keeps records deterministic; models never read the clock themselves.
NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)


def capabilities_body(**overrides):
    body = {
        "gpu_model": "A100",
        "vram_mb": 40960,
        "driver_version": "550.54",
        "runtimes": ["cuda12"],
    }
    body.update(overrides)
    return body


def register_body(**overrides):
    body = {
        "provider_id": "provider-a",
        "capabilities": capabilities_body(),
        "accepted_tiers": ["tier1"],
    }
    body.update(overrides)
    return body


def job_body(**overrides):
    body = {
        "required_vram_mb": 16384,
        "gpu_models": ["A100", "RTX4090"],
        "required_runtime": "cuda12",
        "tier": "tier1",
    }
    body.update(overrides)
    return body


def make_provider():
    request = ProviderRegisterRequest(**register_body())
    return Provider(
        **request.model_dump(),
        status=ProviderStatus.ACTIVE,
        registered_at=NOW,
        last_heartbeat_at=NOW,
    )


def make_job(**overrides):
    fields = {"job_id": "job-1", "status": JobStatus.QUEUED, "created_at": NOW, **job_body()}
    fields.update(overrides)
    return Job(**fields)


def make_lease(**overrides):
    fields = {
        "lease_id": "lease-1",
        "job_id": "job-1",
        "provider_id": "provider-a",
        "status": LeaseStatus.OFFERED,
        "offer_deadline": NOW,
        "created_at": NOW,
    }
    fields.update(overrides)
    return Lease(**fields)


# --- Provider ---------------------------------------------------------------


def test_register_request_accepts_valid_body():
    request = ProviderRegisterRequest(**register_body())
    assert request.provider_id == "provider-a"
    assert request.capabilities.vram_mb == 40960


@pytest.mark.parametrize(
    "body",
    [
        register_body(provider_id=""),
        register_body(capabilities=capabilities_body(gpu_model="")),
        register_body(capabilities=capabilities_body(vram_mb=0)),
        register_body(capabilities=capabilities_body(vram_mb=-1)),
        register_body(unexpected="field"),
    ],
    ids=["empty-id", "empty-gpu-model", "zero-vram", "negative-vram", "extra-field"],
)
def test_register_request_rejects_invalid_body(body):
    with pytest.raises(ValidationError):
        ProviderRegisterRequest(**body)


@pytest.mark.parametrize("field", ["capabilities", "accepted_tiers"])
def test_register_request_requires_field(field):
    body = register_body()
    del body[field]
    with pytest.raises(ValidationError):
        ProviderRegisterRequest(**body)


@pytest.mark.parametrize("seconds", [0, -5])
def test_drain_request_requires_positive_grace_period(seconds):
    with pytest.raises(ValidationError):
        DrainRequest(grace_period_seconds=seconds)


def test_provider_record_serializes_status_as_contract_string():
    data = make_provider().model_dump(mode="json")
    assert data["status"] == "ACTIVE"
    assert data["drain_deadline"] is None


def test_provider_status_assignment_is_validated():
    provider = make_provider()
    provider.status = "STALE"
    assert provider.status is ProviderStatus.STALE
    with pytest.raises(ValidationError):
        provider.status = "DEAD"


def test_provider_statuses_match_contract():
    assert {s.value for s in ProviderStatus} == {"ACTIVE", "STALE", "DRAINING", "DRAINED"}


# --- Job --------------------------------------------------------------------


def test_job_submit_request_accepts_valid_body():
    request = JobSubmitRequest(**job_body())
    assert request.gpu_models == ["A100", "RTX4090"]


def test_job_submit_request_defaults_to_any_gpu_model():
    body = job_body()
    del body["gpu_models"]
    assert JobSubmitRequest(**body).gpu_models == []


@pytest.mark.parametrize(
    "body",
    [
        job_body(required_vram_mb=0),
        job_body(required_runtime=""),
        job_body(tier=""),
        job_body(job_id="client-picked"),
    ],
    ids=["zero-vram", "empty-runtime", "empty-tier", "extra-field"],
)
def test_job_submit_request_rejects_invalid_body(body):
    with pytest.raises(ValidationError):
        JobSubmitRequest(**body)


def test_new_job_record_starts_with_no_attempts_or_lease():
    job = make_job()
    assert job.attempt_count == 0
    assert job.current_lease_id is None
    assert job.rejected_provider_ids == set()
    assert job.last_failure_reason is None


def test_rejected_provider_sets_are_not_shared_between_jobs():
    first, second = make_job(job_id="job-1"), make_job(job_id="job-2")
    first.rejected_provider_ids.add("provider-a")
    assert second.rejected_provider_ids == set()


def test_job_status_assignment_is_validated():
    job = make_job()
    with pytest.raises(ValidationError):
        job.status = "CANCELLED"


def test_job_statuses_match_contract():
    expected = {"QUEUED", "STARTING", "RUNNING", "COMPLETED", "FAILED"}
    assert {s.value for s in JobStatus} == expected


# --- Lease ------------------------------------------------------------------


def test_terminal_lease_statuses_match_contract():
    assert TERMINAL_LEASE_STATUSES == {
        LeaseStatus.REJECTED,
        LeaseStatus.EXPIRED,
        LeaseStatus.REVOKED,
        LeaseStatus.RELEASED,
    }
    assert LeaseStatus.OFFERED not in TERMINAL_LEASE_STATUSES
    assert LeaseStatus.ACTIVE not in TERMINAL_LEASE_STATUSES


def test_new_lease_record_has_no_execution_fields_yet():
    lease = make_lease()
    assert lease.expires_at is None
    assert lease.outcome is None
    assert lease.reason is None
    assert lease.accepted_at is None


def test_lease_status_assignment_is_validated():
    lease = make_lease()
    with pytest.raises(ValidationError):
        lease.status = "RUNNING"


@pytest.mark.parametrize("model", [LeaseActionRequest, RejectRequest, RenewRequest, ReportRequest])
def test_lease_requests_reject_unknown_fields(model):
    # Subclasses must inherit extra="forbid" from LeaseActionRequest.
    body = {
        "provider_id": "provider-a",
        "reason": "LOCAL_BUSY",
        "extend_seconds": 60,
        "outcome": "SUCCESS",
    }
    allowed = set(model.model_fields)
    body = {k: v for k, v in body.items() if k in allowed}
    body["unexpected"] = "field"
    with pytest.raises(ValidationError):
        model(**body)


def test_lease_requests_require_provider_id():
    with pytest.raises(ValidationError):
        LeaseActionRequest(provider_id="")


def test_reject_request_requires_known_reason():
    assert RejectRequest(provider_id="p", reason="LOCAL_BUSY").reason.value == "LOCAL_BUSY"
    with pytest.raises(ValidationError):
        RejectRequest(provider_id="p", reason="BORED")
    with pytest.raises(ValidationError):
        RejectRequest(provider_id="p")


@pytest.mark.parametrize("seconds", [0, -1])
def test_renew_request_requires_positive_extension(seconds):
    with pytest.raises(ValidationError):
        RenewRequest(provider_id="p", extend_seconds=seconds)


def test_report_success_needs_no_reason():
    report = ReportRequest(provider_id="p", outcome="SUCCESS")
    assert report.outcome is LeaseOutcome.SUCCESS
    assert report.reason is None


def test_report_failure_accepts_contract_reason():
    report = ReportRequest(provider_id="p", outcome="FAILURE", reason="DISK_FULL")
    assert report.model_dump(mode="json")["reason"] == "DISK_FULL"


@pytest.mark.parametrize("reason", ["DRAIN_RECLAIM", "OFFER_TIMEOUT", "made_up"])
def test_report_rejects_internal_or_unknown_reasons(reason):
    # Internal reasons are recorded by the Coordinator, never claimed by a Provider.
    with pytest.raises(ValidationError):
        ReportRequest(provider_id="p", outcome="FAILURE", reason=reason)


def test_report_rejects_unknown_outcome():
    with pytest.raises(ValidationError):
        ReportRequest(provider_id="p", outcome="PARTIAL")


def test_lease_record_stores_any_reason_name_as_text():
    lease = make_lease(status=LeaseStatus.EXPIRED, reason="OFFER_TIMEOUT")
    assert lease.reason == "OFFER_TIMEOUT"


def test_capabilities_model_is_reusable_on_its_own():
    assert Capabilities(**capabilities_body()).runtimes == ["cuda12"]
