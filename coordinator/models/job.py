from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class JobStatus(str, Enum):
    QUEUED = "QUEUED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class JobSubmitRequest(BaseModel):
    """Body of POST /jobs. The server generates job_id."""

    model_config = ConfigDict(extra="forbid")

    # ram >= 0
    required_vram_mb: int = Field(gt=0)
    # Empty list means "any GPU model", per contract.
    gpu_models: list[str] = Field(default_factory=list)
    required_runtime: str = Field(min_length=1)
    # Contract: only Providers whose accepted_tiers contain this tier may run it.
    tier: str = Field(min_length=1)


class Job(BaseModel):
    """Stored Job record, owned by the Store."""

    model_config = ConfigDict(validate_assignment=True)

    job_id: str
    status: JobStatus
    required_vram_mb: int
    gpu_models: list[str]
    required_runtime: str
    tier: str

    attempt_count: int = 0
    # At most one non-terminal Lease per Job. Storing its ID lets services reject
    # late reports from old Leases with a single comparison.
    current_lease_id: Optional[str] = None
    # Per-Job exclusion after /reject, so the same Job is not re-offered to the same
    # Provider right away. A set because order does not matter and duplicates are
    # meaningless. This is not a global Provider blacklist.
    rejected_provider_ids: set[str] = Field(default_factory=set)
    created_at: datetime
    # Kept on the Job so a FAILED Job explains itself without looking up old Leases.
    last_failure_reason: Optional[str] = None
