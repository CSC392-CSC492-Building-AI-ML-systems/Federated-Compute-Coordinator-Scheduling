"""Providers HTTP routes: validate input, call services, return responses."""

from datetime import datetime

from fastapi import APIRouter, Depends

from coordinator.api.tools.dependencies import get_received_at, get_store
from coordinator.api.tools.errors import ApiError
from coordinator.models.provider import ProviderRegisterRequest, ProviderRegisterResponse
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
