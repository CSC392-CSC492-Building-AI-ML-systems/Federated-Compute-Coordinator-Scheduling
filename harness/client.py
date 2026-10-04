"""Coordinator HTTP client. One function per endpoint."""

import httpx

from harness import errors


class CoordinatorClient:
    # TODO: Use coordinator.models enums for statuses and reasons once they exist.

    def __init__(self, base_url, api_key=None, transport=None):
        self.http = httpx.AsyncClient(
            base_url=base_url,
            headers={"X-API-Key": api_key} if api_key else None,
            transport=transport,
            timeout=5.0,
        )

    async def close(self):
        """Close the HTTP client."""
        await self.http.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def _request(self, method, path, json=None):
        """Send one request and return the parsed JSON body."""
        response = await self.http.request(method, path, json=json)

        if response.is_error:
            try:
                error = response.json()["error"]
                code, message = error["code"], error["message"]
            except (ValueError, KeyError, TypeError):
                code, message = "UNKNOWN", response.text
            raise errors.CoordinatorError(response.status_code, code, message)

        return response.json()

    # --- Providers: register, keep alive, inspect, leave ---

    async def register_provider(self, data):
        """Register a provider.

        `data`: provider_id, capabilities {gpu_model, vram_mb, driver_version,
        runtimes}, accepted_tiers.

        Returns provider_id, status, registered_at, last_heartbeat_at.
        """
        return await self._request("POST", "/providers", json=data)

    async def heartbeat(self, provider_id, provider_time=None):
        """Send a provider heartbeat. Returns provider_id, status, server_received_at.

        `provider_time` is an optional ISO 8601 string (e.g. `now.isoformat()`),
        only logged by the coordinator, so a skewed clock can be sent without
        affecting stale detection.
        """
        body = {}
        if provider_time is not None:
            body["provider_time"] = provider_time
        return await self._request("POST", f"/providers/{provider_id}/heartbeat", json=body)

    async def get_provider(self, provider_id):
        """Get the full provider record."""
        return await self._request("GET", f"/providers/{provider_id}")

    async def drain(self, provider_id, grace_period_seconds):
        """Start draining: no new offers; running leases may finish until the deadline.

        Repeating the call does not extend the deadline.

        Returns provider_id, status, drain_deadline.
        """
        return await self._request(
            "POST",
            f"/providers/{provider_id}/drain",
            json={"grace_period_seconds": grace_period_seconds},
        )

    async def recover(self, provider_id, reason):
        """Return an UNHEALTHY provider to ACTIVE. Returns provider_id, status.

        PROPOSAL in the contract: only exists if UNHEALTHY is adopted for v1.
        """
        return await self._request(
            "POST", f"/providers/{provider_id}/recover", json={"reason": reason}
        )

    # Leases, in lifecycle order: offer -> accept/reject -> started -> renew -> report

    async def get_lease_offers(self, provider_id):
        """Return this provider's OFFERED leases, each with job_requirements."""
        body = await self._request("GET", f"/providers/{provider_id}/lease-offers")
        return body["leases"]

    async def accept(self, lease_id, provider_id):
        """Accept a lease.

        Returns lease_status, expires_at, job_status, attempt_count.
        """
        return await self._request(
            "POST", f"/leases/{lease_id}/accept", json={"provider_id": provider_id}
        )

    async def reject(self, lease_id, provider_id, reason):
        """Reject an offered lease. Uses no attempt; the job stays QUEUED.

        `reason` is LOCAL_BUSY, TEMP_UNAVAILABLE, POLICY_REJECT, CAPABILITY_MISMATCH
        or RESOURCE_UNAVAILABLE. The provider is added to the job's
        rejected_provider_ids.

        Returns lease_status, job_status.
        """
        return await self._request(
            "POST",
            f"/leases/{lease_id}/reject",
            json={"provider_id": provider_id, "reason": reason},
        )

    async def started(self, lease_id, provider_id):
        """Mark a lease as started.

        Returns lease_status, job_status (RUNNING).
        """
        return await self._request(
            "POST", f"/leases/{lease_id}/started", json={"provider_id": provider_id}
        )

    async def renew(self, lease_id, provider_id, extend_seconds):
        """Extend an active lease. Heartbeats do not extend a lease; only this does.

        Returns the new expires_at, never past the provider's drain_deadline.
        """
        return await self._request(
            "POST",
            f"/leases/{lease_id}/renew",
            json={"provider_id": provider_id, "extend_seconds": extend_seconds},
        )

    async def report(self, lease_id, provider_id, outcome, reason=None):
        """Report the outcome of a lease. Returns lease_status, outcome, job_status.

        `outcome` is SUCCESS or FAILURE. `reason` is optional: EXECUTION_ERROR,
        RUNTIME_ERROR, CAPABILITY_MISMATCH, DISK_FULL or ARTIFACT_UPLOAD_FAILED.
        """
        body = {"provider_id": provider_id, "outcome": outcome}
        if reason is not None:
            body["reason"] = reason
        return await self._request("POST", f"/leases/{lease_id}/report", json=body)

    async def get_lease(self, lease_id):
        """Get the full lease record."""
        return await self._request("GET", f"/leases/{lease_id}")

    # Jobs

    async def submit_job(self, data):
        """Submit a job.

        `data`: required_vram_mb, gpu_models (empty list = any model),
        required_runtime, tier, and optionally max_attempts (still an open decision).

        Returns the full job record, including job_id, status, attempt_count,
        current_lease_id.
        """
        return await self._request("POST", "/jobs", json=data)

    async def get_job(self, job_id):
        """Get the full job record. Raises CoordinatorError 404 JOB_NOT_FOUND."""
        return await self._request("GET", f"/jobs/{job_id}")

    # Read status

    async def get_status(self):
        """Get counts per state for providers, jobs and leases."""
        return await self._request("GET", "/status")
