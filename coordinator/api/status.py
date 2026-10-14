"""Status HTTP routes: validate input, call services, return responses."""

from fastapi import APIRouter, Depends

from coordinator.api.tools.dependencies import get_store
from coordinator.models.status import StatusSnapshot
from coordinator.services import status_service
from coordinator.store import Store

router = APIRouter()


@router.get("/status", status_code=200, response_model=StatusSnapshot)
async def get_status(store: Store = Depends(get_store)):
    """Counts per state. Read-only: asking for status never triggers a transition."""
    return await status_service.get_status(store)
