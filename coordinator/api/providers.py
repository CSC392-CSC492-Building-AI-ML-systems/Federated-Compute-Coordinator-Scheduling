"""Providers HTTP routes: validate input, call services, return responses."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends

from coordinator.api.tools.dependencies import get_received_at, get_store
from coordinator.api.tools.errors import ApiError
from coordinator.models.provider import (
    HeartbeatRequest,
    HeartbeatResponse,
    ProviderRegisterRequest,
    ProviderRegisterResponse,
)
from coordinator.services import provider_service
from coordinator.store import Store

router = APIRouter()


# 201 Created
@router.post("/providers", status_code=201, response_model=ProviderRegisterResponse)
async def register_provider(
    body: ProviderRegisterRequest,
    store: Store = Depends(get_store),
    received_at: datetime = Depends(get_received_at),
):
    # By the time we get here FastAPI has already validated `body` (422 otherwise),
    # so the service only sees well-formed input. :0
    try:
        provider = await provider_service.register_provider(store, body, received_at)
    except provider_service.ProviderAlreadyExists as exc:
        # Translating to HTTP is the route's job, not the service's.
        # conflict == 409
        raise ApiError(409, "DUPLICATE_ID", str(exc)) from exc
    return ProviderRegisterResponse.model_validate(provider.model_dump())


@router.post(
    "/providers/{provider_id}/heartbeat", status_code=200, response_model=HeartbeatResponse
)
async def heartbeat(
    provider_id: str,
    body: Optional[HeartbeatRequest] = None,
    store: Store = Depends(get_store),
    received_at: datetime = Depends(get_received_at),
):
    """Receive a heartbeat; the service owns time updates and state transitions."""
    try:
        provider = await provider_service.heartbeat(
            store, provider_id, body if body is not None else HeartbeatRequest(), received_at
        )
    except provider_service.ProviderNotFound as exc:
        raise ApiError(404, "NOT_FOUND", str(exc)) from exc
    except provider_service.ProviderDrained as exc:
        raise ApiError(409, "INVALID_STATE_TRANSITION", str(exc)) from exc
    return HeartbeatResponse(
        provider_id=provider.provider_id,
        status=provider.status,
        server_received_at=received_at,
    )
