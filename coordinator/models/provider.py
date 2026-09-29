from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class ProviderStatus(str, Enum):
    ACTIVE = "ACTIVE"
    STALE = "STALE"
    DRAINING = "DRAINING"
    DRAINED = "DRAINED"


class Capabilities(BaseModel):
    """
    {
    "gpu_model": "NVIDIA RTX 4090",
    "vram_mb": 24576,
    "driver_version": "550.54",
    "runtimes": ["cuda", "docker", "pytorch"]
    }
    """

    gpu_model: str = Field(min_length=1)
    vram_mb: int = Field(gt=0)
    driver_version: str
    runtimes: list[str]


class ProviderRegisterRequest(BaseModel):
    """
    {
    "provider_id": "provider-001",
    "capabilities": {
        "gpu_model": "NVIDIA RTX 4090",
        "vram_mb": 24576,
        "driver_version": "550.54",
        "runtimes": ["cuda", "docker", "pytorch"]
        }
    }
    """

    # this line means client cannot add more beyond this schema
    model_config = ConfigDict(extra="forbid")

    provider_id: str = Field(min_length=1)
    capabilities: Capabilities
    # Contract: matching requires the Job's tier to be in this list.
    accepted_tiers: list[str]


class DrainRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grace_period_seconds: int = Field(gt=0)


class Provider(BaseModel):
    """ """

    # cannot trust external request, but internal just check fields
    model_config = ConfigDict(validate_assignment=True)

    provider_id: str
    status: ProviderStatus
    capabilities: Capabilities
    accepted_tiers: list[str]
    registered_at: datetime
    last_heartbeat_at: datetime

    drain_deadline: Optional[datetime] = None


class ProviderRegisterResponse(BaseModel):
    """Body of the 201 response to POST /providers (contract section 4).

    A separate model so the response only exposes these four fields, even if the
    stored Provider record gains internal fields later.
    """

    provider_id: str
    status: ProviderStatus
    registered_at: datetime
    last_heartbeat_at: datetime
