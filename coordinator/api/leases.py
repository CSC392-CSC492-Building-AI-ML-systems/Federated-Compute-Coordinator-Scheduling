"""Leases HTTP routes: validate input, call services, return responses."""

from fastapi import APIRouter, Depends

from coordinator.api.tools.dependencies import get_store
from coordinator.api.tools.errors import ApiError
from coordinator.models.lease import Lease, ProviderLeaseOffersResponse, RejectRequest
from coordinator.services.lease_service import (
    InvalidLeaseState,
    LeaseNotFound,
    LeaseProviderMismatch,
    ProviderNotFound,
    get_provider_lease_offers,
    reject_lease,
)
from coordinator.store import Store

router = APIRouter()

# TODO: Define endpoints and request/response models.
# Keep state changes in services, not in route handlers.


@router.get(
    "/providers/{provider_id}/lease-offers",
    status_code=200,
    response_model=ProviderLeaseOffersResponse,
)
async def get_lease_offers(provider_id: str, store: Store = Depends(get_store)):
    """Endpoint to obtain all offered leases and their job requirements for a specific provider."""
    try:
        return await get_provider_lease_offers(store, provider_id)
    except ProviderNotFound as exc:
        raise ApiError(404, "PROVIDER_NOT_FOUND", str(exc)) from exc


@router.post("/leases/{lease_id}/reject", status_code=200, response_model=Lease)
async def reject_lease_endpoint(
    lease_id: str, request: RejectRequest, store: Store = Depends(get_store)
):
    try:
        lease = await reject_lease(store, lease_id, request)
        return lease
    except LeaseNotFound as e:
        raise ApiError(404, "LEASE_NOT_FOUND", str(e))
    except LeaseProviderMismatch as e:
        raise ApiError(403, "LEASE_PROVIDER_MISMATCH", str(e))
    except InvalidLeaseState as e:
        raise ApiError(409, "INVALID_LEASE_STATE", str(e))
