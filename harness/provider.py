"""One simulated provider node. `behavior` selects normal operation or a failure mode.

The normal lifecycle lives here. At the step a failure mode changes, the provider
branches on `behavior` and calls the matching function in harness.behaviors.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import httpx

from harness.behaviors import execution, liveness
from harness.behaviors import lease as lease_behaviors
from harness.errors import CoordinatorError

# Defaults matching shared.yaml. The runner passes the real
# values in by `params`.
DEFAULT_JOB_DURATION_SEC = 5
HEARTBEAT_INTERVAL_SEC = 1
RENEW_INTERVAL_SEC = 3
# run() ticks once per second, looks for offers every tick,
# Also finishes jobs, looks for leases etc. at every heartbeat interval
# This interval is used for the ticks which package the above functionality
TICK_SEC = 1


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

        self.params = params or {}
        self.heartbeat_interval = timedelta(
            seconds=self.params.get("heartbeat_interval_sec", HEARTBEAT_INTERVAL_SEC)
        )
        self.renew_interval_sec = self.params.get("renew_interval_sec", RENEW_INTERVAL_SEC)
        # This here is because job in a scenario takes the same time (shared.yaml or the scenario)
        # Will have to change it if we make jobs differ in their timing
        self.job_duration_sec = self.params.get("duration_sec", DEFAULT_JOB_DURATION_SEC)
        self.last_heartbeat_at = None
        self.last_renewed_at = None

        # Set to True once registered, set False to make the provider vanish
        self.is_heartbeating = False
        self.lease_id = None
        # We can make this be the current time of the provider + the job duration
        self.job_finishes_at = None
        self.events = []
        # Status last reported by the coordinator in respect to this provider
        self.status = None

        # --- NEW Behaviour states ---
        self.heartbeat_count = 0  # for disappear
        self.job_started_at = None  # for drain_mid_job
        # to change after yaml param things are changed again
        self.disappear_after_heartbeats = self.params.get("disappear_after_heartbeats", 2)
        self.skew_seconds = self.params.get("skew_seconds", 3600)
        self.reject_reason = self.params.get("reject_reason", "LOCAL_BUSY")
        self.actual_vram_mb = self.params.get("actual_vram_mb", None)
        self.actual_vram_mb = self.params.get("actual_vram_mb", None)
        self.drain_after_sec = self.params.get("drain_after_sec", None)
        self.grace_period_seconds = self.params.get("grace_period_seconds", None)

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
        """One tick: heartbeat if due, then take an offer, finish the job or renew it."""
        if not self.is_heartbeating:
            return
        if self.behavior == "disappear":
            if self.heartbeat_count >= self.disappear_after_heartbeats:
                await liveness.disappear(self, now)
                return
        try:
            if self.heartbeat_due(now):
                await self.heartbeat(now)
            if self.lease_id is None:
                if self.status == "ACTIVE":
                    await self.check_offers(now)
            elif now >= self.job_finishes_at:
                await self.finish_job(now)
            elif now - self.last_renewed_at >= timedelta(seconds=self.renew_interval_sec):
                await self.renew(now)

            # To discuss if logic to be rather shifted to behaviour functions
            if (
                self.behavior == "drain_mid_job"
                and self.lease_id is not None
                and self.job_started_at is not None
                and self.status == "ACTIVE"
                and self.drain_after_sec is not None
                and (now - self.job_started_at).total_seconds() >= self.drain_after_sec
            ):
                await execution.drain_mid_job(self, now)

        except CoordinatorError as error:
            self.log_error(now, error)
        except httpx.TransportError as error:
            # Coordinator slow or unreachable (e.g. past the client's timeout):
            # keep running and retry next tick.
            self.log_network_error(now, error)

    # Handle the DRAINING status and stop heartbeating
    # once drained. We know the drain has completed when
    # a heartbeat gets a 409 while DRAINING.
    # Other 409 sources shouldn't stop the heartbeating.
    async def heartbeat(self, now):
        if self.behavior == "clock_skew":
            provider_time = liveness.clock_skew(self, now)
        else:
            provider_time = now.isoformat()
        try:
            response = await self.client.heartbeat(self.provider_id, provider_time)
        except CoordinatorError as error:
            if self.status == "DRAINING" and error.status_code == 409:
                self.status = "DRAINED"
                self.is_heartbeating = False
                self.log_event(now, "drained")
            raise
        self.status = response["status"]
        self.last_heartbeat_at = now
        self.heartbeat_count += 1
        self.log_event(now, "heartbeat", status=self.status)

    def heartbeat_due(self, now):
        if self.last_heartbeat_at is None:
            return True
        return now - self.last_heartbeat_at >= self.heartbeat_interval

    async def check_offers(self, now):
        offers = await self.client.get_lease_offers(self.provider_id)
        if not offers:
            return
        # MVP: at most one live lease per provider, so take the first offer.
        offer = offers[0]
        lease_id = offer["lease_id"]

        if self.behavior == "reject_lease":
            await lease_behaviors.reject_lease(self, offer, now)
            return

        await self.client.accept(lease_id, self.provider_id)
        # Track the lease as soon as it is accepted, so a failed `started` call
        # does not leave the coordinator holding a lease this provider forgot.
        self.lease_id = lease_id
        # The job itself is the sleep: it is done once now reaches job_finishes_at
        self.job_finishes_at = now + timedelta(seconds=self.job_duration_sec)
        self.job_started_at = now
        self.last_renewed_at = now
        self.log_event(now, "accepted", lease_id=lease_id, job_id=offer["job_id"])
        # SUCCESS can only occur when the job is in the RUNNING state
        await self.client.started(lease_id, self.provider_id)

    async def renew(self, now):
        """Ask for more time on the running lease. Heartbeats do not extend a lease.
        Renew adds extend_seconds to the current expires_at, so extending by the
        renew interval keeps the lease the same distance ahead of now.
        """
        # Record the attempt before calling, so a failed renew waits for the next
        # interval instead of retrying every tick. Missing one renew is safe (R4).
        self.last_renewed_at = now
        try:
            await self.client.renew(self.lease_id, self.provider_id, self.renew_interval_sec)
        except CoordinatorError as error:
            # 409: the lease already expired or was revoked, so the job no longer belongs
            # to this provider
            if error.status_code == 409:
                self.log_event(now, "lease_lost", lease_id=self.lease_id)
                self.lease_id = None
                self.job_started_at = None
            raise
        self.log_event(now, "renewed", lease_id=self.lease_id)

    async def finish_job(self, now):
        """Report completion of the lease and become ready to accept another one."""
        temp_lease_id = self.lease_id
        self.lease_id = None
        self.job_started_at = None

        if self.behavior == "capability_lie":
            await lease_behaviors.capability_lie(self, temp_lease_id, now)
            return
        if self.behavior == "upload_fail":
            await execution.upload_fail(self, temp_lease_id, now)
            return
        if self.behavior == "disk_full":
            await execution.disk_full(self, temp_lease_id, now)
            return

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
            await asyncio.sleep(TICK_SEC)
