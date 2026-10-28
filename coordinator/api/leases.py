"""Leases HTTP routes: validate input, call services, return responses."""

from fastapi import APIRouter, Depends

from coordinator.api.tools.dependencies import get_store
from coordinator.api.tools.errors import ApiError
from coordinator.models.lease import Lease, ProviderLeaseOffersResponse
from coordinator.services.lease_service import get_provider_lease_offers
from coordinator.services.provider_service import ProviderNotFound
from coordinator.store import Store

router = APIRouter()

# TODO: Define endpoints and request/response models.
# Keep state changes in services, not in route handlers.


@router.get("/{provider_id}/lease-offers", response_model=ProviderLeaseOffersResponse)
async def get_lease_offers(provider_id: str, store: Store = Depends(get_store)):
    try:
        return await get_provider_lease_offers(store, provider_id)
    except ProviderNotFound as exc:
        raise ApiError(404, "PROVIDER_NOT_FOUND", str(exc)) from exc
