"""Leases HTTP routes: validate input, call services, return responses."""

from datetime import datetime

from fastapi import APIRouter, Depends

from coordinator.api.tools.dependencies import get_received_at, get_settings, get_store
from coordinator.api.tools.errors import ApiError
from coordinator.models.lease import (
    AcceptResponse,
    Lease,
    LeaseActionRequest,
    ProviderLeaseOffersResponse,
    RejectRequest,
    RejectResponse,
    StartedResponse,
)
from coordinator.services.lease_service import (
    InvalidJobState,
    InvalidLeaseState,
    LeaseExpired,
    LeaseNotCurrent,
    LeaseNotFound,
    LeaseProviderMismatch,
    OfferExpired,
    ProviderNotActive,
    ProviderNotFound,
    accept_lease,
    get_lease,
    get_provider_lease_offers,
    mark_lease_started,
    reject_lease,
)
from coordinator.settings import Settings
from coordinator.store import Store

router = APIRouter()


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


@router.post(
    "/leases/{lease_id}/reject",
    status_code=200,
    response_model=RejectResponse,
)
async def reject_lease_endpoint(
    lease_id: str,
    request: RejectRequest,
    store: Store = Depends(get_store),
    received_at: datetime = Depends(get_received_at),
):
    try:
        return await reject_lease(store, lease_id, request, now=received_at)
    except LeaseNotFound as e:
        raise ApiError(404, "LEASE_NOT_FOUND", str(e)) from e
    except LeaseProviderMismatch as e:
        raise ApiError(403, "LEASE_PROVIDER_MISMATCH", str(e)) from e
    except InvalidLeaseState as e:
        raise ApiError(409, "INVALID_LEASE_STATE", str(e)) from e


@router.get("/leases/{lease_id}", status_code=200, response_model=Lease)
async def get_lease_endpoint(lease_id: str, store: Store = Depends(get_store)):
    """Endpoint to read one Lease, whatever its status."""
    # 1, lease found -> 200 with the full Lease record
    # 2, lease not found -> 404 LEASE_NOT_FOUND, same code as the reject route
    try:
        return await get_lease(store, lease_id)
    except LeaseNotFound as exc:
        raise ApiError(404, "LEASE_NOT_FOUND", str(exc)) from exc


@router.post("/leases/{lease_id}/accept", status_code=200, response_model=AcceptResponse)
async def accept_lease_endpoint(
    lease_id: str,
    request: LeaseActionRequest,
    store: Store = Depends(get_store),
    received_at: datetime = Depends(get_received_at),
    settings: Settings = Depends(get_settings),
):
    """Endpoint for a Provider to accept an offered Lease."""
    try:
        return await accept_lease(
            store, lease_id, request, received_at, settings.lease_duration_seconds
        )
    except LeaseNotFound as exc:
        raise ApiError(404, "LEASE_NOT_FOUND", str(exc)) from exc
    except LeaseProviderMismatch as exc:
        # 403 matches the reject route. The team may switch both to 409.
        raise ApiError(403, "LEASE_PROVIDER_MISMATCH", str(exc)) from exc
    except InvalidLeaseState as exc:
        raise ApiError(409, "INVALID_LEASE_STATE", str(exc)) from exc
    except OfferExpired as exc:
        raise ApiError(409, "OFFER_EXPIRED", str(exc)) from exc
    except ProviderNotActive as exc:
        raise ApiError(409, "PROVIDER_NOT_ACTIVE", str(exc)) from exc
    except LeaseNotCurrent as exc:
        raise ApiError(409, "LEASE_NOT_CURRENT", str(exc)) from exc


@router.post("/leases/{lease_id}/started", status_code=200, response_model=StartedResponse)
async def mark_lease_started_endpoint(
    lease_id: str,
    request: LeaseActionRequest,
    store: Store = Depends(get_store),
    received_at: datetime = Depends(get_received_at),
):
    """Endpoint for a Provider to confirm that its accepted Job is running."""
    try:
        return await mark_lease_started(store, lease_id, request, received_at)
    except LeaseNotFound as exc:
        raise ApiError(404, "LEASE_NOT_FOUND", str(exc)) from exc
    except LeaseProviderMismatch as exc:
        # 403 matches the reject route. The team may switch both to 409.
        raise ApiError(403, "LEASE_PROVIDER_MISMATCH", str(exc)) from exc
    except InvalidLeaseState as exc:
        raise ApiError(409, "INVALID_LEASE_STATE", str(exc)) from exc
    except LeaseExpired as exc:
        raise ApiError(409, "LEASE_EXPIRED", str(exc)) from exc
    except ProviderNotActive as exc:
        raise ApiError(409, "PROVIDER_NOT_ACTIVE", str(exc)) from exc
    except LeaseNotCurrent as exc:
        raise ApiError(409, "LEASE_NOT_CURRENT", str(exc)) from exc
    except InvalidJobState as exc:
        raise ApiError(409, "INVALID_JOB_STATE", str(exc)) from exc
