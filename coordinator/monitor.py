import asyncio
import logging
from datetime import datetime
from typing import Callable

from coordinator.services import provider_service
from coordinator.settings import Settings
from coordinator.store import Store

logger = logging.getLogger(__name__)


async def run_monitor_tick(store: Store, now: datetime, settings: Settings) -> None:
    # 1 Heartbeat timeout: ACTIVE -> STALE, DRAINING -> DRAINED.
    await provider_service.expire_heartbeats(store, now, settings.heartbeat_timeout_seconds)
    # 2 Drain end: DRAINING with no live Lease, or past drain_deadline -> DRAINED.
    #    Runs after step 1, so it only sees Providers that are still in contact.
    await provider_service.finish_drains(store, now)
    # TODO: offer expiry and execution (Lease) timeout once Leases exist.


async def monitor_loop(store: Store, clock: Callable[[], datetime], settings: Settings) -> None:
    while True:
        try:
            await run_monitor_tick(store, clock(), settings)
        except Exception:
            # One failed tick must not stop monitoring for good: log it and try again.
            logger.exception("monitor tick failed")

        await asyncio.sleep(settings.monitor_interval_seconds)
