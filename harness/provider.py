"""One simulated provider node. `behavior` selects normal operation or a failure mode.

The normal lifecycle lives here. At the step a failure mode changes, the provider
branches on `behavior` and calls the matching function in harness.behaviors.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import httpx

from harness.errors import CoordinatorError

# Can remove this if we decide to have the duration of the job directly
# tied to the job as an attribute
DEFAULT_JOB_DURATION_SEC = 4

HEARTBEAT_INTERVAL_SEC = 1
# Also finishes jobs, looks for leases etc. at every heartbeat interval
# This interval is used for the ticks which package the above functionality


class FakeProvider:
    def __init__(
        self,
        client,
        provider_id,
        gpu_model,
        vram_mb,
        runtimes,
        accepted_tiers,
        driver_version="1.0",
        behavior="normal",
        params=None,
    ):
        self.client = client
        self.provider_id = provider_id
        self.capabilities = {
            "gpu_model": gpu_model,
            "vram_mb": vram_mb,
            "driver_version": driver_version,
            "runtimes": runtimes,
        }
        self.accepted_tiers = accepted_tiers
        self.behavior = behavior
        self.params = params
        # Set to True once registered, set False to make the provider vanish
        self.is_heartbeating = False
        self.lease_id = None
        # We can make this be the current time of the provider + the job duration
        self.job_finishes_at = None
        self.events = []
        # Status last reported by the coordinator in respect to this provider
        self.status = None

    async def register(self, now):
        try:
            response = await self.client.register_provider(
                {
                    "provider_id": self.provider_id,
                    "capabilities": self.capabilities,
                    "accepted_tiers": self.accepted_tiers,
                }
            )
        except CoordinatorError as error:
            self.log_error(now, error)
            return
        except httpx.TransportError as error:
            self.log_network_error(now, error)
            return
        self.status = response["status"]
        self.is_heartbeating = True
        self.log_event(now, "registered", status=self.status)

    async def drain(self, now, grace_period_seconds):
        """Commences a drain. Tries to finish the current job in the grace period,
        takes no new ones, then stops heartbeating once drained."""
        try:
            response = await self.client.drain(self.provider_id, grace_period_seconds)
        except CoordinatorError as error:
            self.log_error(now, error)
            return
        except httpx.TransportError as error:
            self.log_network_error(now, error)
            return
        self.status = response["status"]
        self.log_event(
            now, "drain_started", status=self.status, grace_period_seconds=grace_period_seconds
        )

    async def tick(self, now):
        """One tick: heartbeat, then take an offer or finish the current job."""
        if not self.is_heartbeating:
            return
        try:
            await self.heartbeat(now)
            if self.lease_id is None:
                if self.status == "ACTIVE":
                    await self.check_offers(now)
            elif now >= self.job_finishes_at:
                await self.finish_job(now)
        except CoordinatorError as error:
            self.log_error(now, error)
        except httpx.TransportError as error:
            # Coordinator slow or unreachable (e.g. past hardcoded timout), keep running and retry next tick.
            self.log_network_error(now, error)

    # Handle the DRAINING status and stop heartbeating
    # once drained. We know the drain has completed when
    # a heartbeat gets a 409 while DRAINING.
    # Other 409 sources shouldn't stop the heartbeating.
    async def heartbeat(self, now):
        try:
            response = await self.client.heartbeat(self.provider_id, now.isoformat())
        except CoordinatorError as error:
            if self.status == "DRAINING" and error.status_code == 409:
                self.status = "DRAINED"
                self.is_heartbeating = False
                self.log_event(now, "drained")
            raise
        self.status = response["status"]
        self.log_event(now, "heartbeat", status=self.status)

    async def check_offers(self, now):
        offers = await self.client.get_lease_offers(self.provider_id)
        if not offers:
            return
        # MVP: at most one live lease per provider, so take the first offer.
        offer = offers[0]
        lease_id = offer["lease_id"]
        # TODO: The job itself is the sleep, but right now this
        # just assigns 4 seconds to all of them. This will probably change once job.py
        # or something of the sort is written.
        duration = offer.get("duration_sec", DEFAULT_JOB_DURATION_SEC)
        await self.client.accept(lease_id, self.provider_id)
        # Track the lease as soon as it is accepted, so a failed `started` call
        # does not leave the coordinator holding a lease this provider forgot.
        self.lease_id = lease_id
        self.job_finishes_at = now + timedelta(seconds=duration)
        self.log_event(now, "accepted", lease_id=lease_id, job_id=offer["job_id"])
        # SUCCESS can only occur when the job is in the RUNNING state
        await self.client.started(lease_id, self.provider_id)

    async def finish_job(self, now):
        """Report completion of the lease and become ready to accept another one."""
        temp_lease_id = self.lease_id
        self.lease_id = None
        await self.client.report(temp_lease_id, self.provider_id, "SUCCESS")
        self.log_event(now, "reported", lease_id=temp_lease_id, outcome="SUCCESS")

    def log_error(self, now, error):
        """Log a request the coordinator rejected, instead of crashing the run."""
        self.log_event(now, "request_failed", http_status=error.status_code, code=error.code)

    def log_network_error(self, now, error):
        """Log a request that never got a response (timeout, connection refused)."""
        self.log_event(now, "network_error", error=type(error).__name__)

    def log_event(self, now, event, **details):
        """Log events, including accepting a lease, finishing a job, and heartbeating"""
        self.events.append(
            {"t": now.isoformat(), "provider": self.provider_id, "event": event, **details}
        )

    async def run(self):
        """Register, then heartbeat and handle lease offers until the heartbeat stops."""
        await self.register(datetime.now(timezone.utc))
        while self.is_heartbeating:
            await self.tick(datetime.now(timezone.utc))
            await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
