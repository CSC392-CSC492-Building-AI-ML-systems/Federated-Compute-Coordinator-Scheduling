from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class LeaseStatus(str, Enum):
    OFFERED = "OFFERED"
    ACTIVE = "ACTIVE"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    REVOKED = "REVOKED"
    RELEASED = "RELEASED"


# frozen hash set, dont change
TERMINAL_LEASE_STATUSES = frozenset(
    {LeaseStatus.REJECTED, LeaseStatus.EXPIRED, LeaseStatus.REVOKED, LeaseStatus.RELEASED}
)


class LeaseOutcome(str, Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class RejectReason(str, Enum):
    LOCAL_BUSY = "LOCAL_BUSY"
    TEMP_UNAVAILABLE = "TEMP_UNAVAILABLE"
    POLICY_REJECT = "POLICY_REJECT"
    CAPABILITY_MISMATCH = "CAPABILITY_MISMATCH"
    RESOURCE_UNAVAILABLE = "RESOURCE_UNAVAILABLE"


class FailureReason(str, Enum):
    EXECUTION_ERROR = "EXECUTION_ERROR"
    RUNTIME_ERROR = "RUNTIME_ERROR"
    CAPABILITY_MISMATCH = "CAPABILITY_MISMATCH"
    DISK_FULL = "DISK_FULL"
    ARTIFACT_UPLOAD_FAILED = "ARTIFACT_UPLOAD_FAILED"


class InternalReason(str, Enum):
    OFFER_TIMEOUT = "OFFER_TIMEOUT"
    EXECUTION_TIMEOUT = "EXECUTION_TIMEOUT"
    PROVIDER_STALE = "PROVIDER_STALE"
    DRAIN_STARTED = "DRAIN_STARTED"
    DRAIN_RECLAIM = "DRAIN_RECLAIM"


class LeaseActionRequest(BaseModel):
    """Body of accept and started. Other actions extend it."""

    model_config = ConfigDict(extra="forbid")

    provider_id: str = Field(min_length=1)


class RejectRequest(LeaseActionRequest):
    """Body of POST /leases/{lease_id}/reject."""

    reason: RejectReason


class RenewRequest(LeaseActionRequest):
    """Body of POST /leases/{lease_id}/renew."""

    extend_seconds: int = Field(gt=0)  # gt=0 is a proposal.


class ReportRequest(LeaseActionRequest):
    """Body of POST /leases/{lease_id}/report."""

    outcome: LeaseOutcome

    reason: Optional[FailureReason] = None


class Lease(BaseModel):
    """Stored Lease record, owned by the Store."""

    model_config = ConfigDict(validate_assignment=True)

    lease_id: str
    job_id: str
    provider_id: str
    status: LeaseStatus
    offer_deadline: datetime
    # None until accept; then set from the configured execution lease duration.
    expires_at: Optional[datetime] = None
    outcome: Optional[LeaseOutcome] = None
    # Plain str because it may hold a RejectReason, FailureReason, or InternalReason.
    # The enums above validate input; storage only needs the name.
    reason: Optional[str] = None
    created_at: datetime
    # One timestamp per transition makes timelines in tests and the Harness report
    # easy to read without a separate event log.
    accepted_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    released_at: Optional[datetime] = None
