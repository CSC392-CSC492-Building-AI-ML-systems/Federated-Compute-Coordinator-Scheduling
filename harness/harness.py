"""
Fake-Provider Harness (Option 7b)

Simulates N provider nodes talking to a running coordinator, each following
a configured "behavior" (normal or one of 7 failure modes). Scenarios are
loaded from YAML so runs are repeatable and configurable.

Usage:
    python harness/run_harness.py harness/scenarios/default.yaml
"""
import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx


@dataclass
class ProviderConfig:
    name: str
    behavior: str
    capabilities: Dict[str, Any]
    tiers: List[str]
    reclaim_policy: str = "graceful"
    params: Dict[str, Any] = field(default_factory=dict)


class FakeProvider:
    """Drives one simulated node through registration, heartbeats, and the
    lease lifecycle, injecting whatever failure the config asks for."""

    def __init__(self, client: httpx.AsyncClient, cfg: ProviderConfig,
                 heartbeat_interval: float, poll_interval: float, log: List[dict]):
        self.client = client
        self.cfg = cfg
        self.heartbeat_interval = heartbeat_interval
        self.poll_interval = poll_interval
        self.log = log
        self.id: Optional[str] = None
        self.api_key: Optional[str] = None
        self.heartbeat_count = 0
        self._stopped = False

    def _record(self, event: str, **kw):
        self.log.append({"t": round(time.time(), 3), "provider": self.cfg.name,
                          "behavior": self.cfg.behavior, "event": event, **kw})

    async def register(self):
        resp = await self.client.post("/providers", json={
            "name": self.cfg.name,
            "capabilities": self.cfg.capabilities,
            "tiers": self.cfg.tiers,
            "reclaim_policy": self.cfg.reclaim_policy,
        })
        resp.raise_for_status()
        data = resp.json()
        self.id, self.api_key = data["id"], data["api_key"]
        self._record("registered", provider_id=self.id)

    def _headers(self):
        return {"X-API-Key": self.api_key}

    async def heartbeat_loop(self):
        disappear_after = self.cfg.params.get("disappear_after_heartbeats")
        skew = self.cfg.params.get("skew_seconds", 0)
        while not self._stopped:
            if disappear_after is not None and self.heartbeat_count >= disappear_after:
                self._record("stopped_heartbeating")
                return  # node "disappears": simply stop pinging forever
            client_time = time.time() + skew if skew else None
            try:
                await self.client.post(f"/providers/{self.id}/heartbeat",
                                        json={"client_time": client_time},
                                        headers=self._headers())
                self.heartbeat_count += 1
                if skew:
                    self._record("heartbeat_sent_skewed", skew_seconds=skew)
            except httpx.HTTPError as e:
                self._record("heartbeat_error", error=str(e))
            await asyncio.sleep(self.heartbeat_interval)

    async def lease_loop(self):
        while not self._stopped:
            try:
                resp = await self.client.get(f"/providers/{self.id}/leases",
                                              params={"status": "offered"})
                offers = resp.json()
            except httpx.HTTPError as e:
                self._record("poll_error", error=str(e))
                await asyncio.sleep(self.poll_interval)
                continue

            for lease in offers:
                await self._handle_offer(lease)

            await asyncio.sleep(self.poll_interval)

    async def _handle_offer(self, lease: dict):
        lid = lease["id"]
        behavior = self.cfg.behavior

        if behavior == "reject_lease":
            await self.client.post(f"/leases/{lid}/respond", json={"accept": False},
                                    headers=self._headers())
            self._record("rejected_lease", lease_id=lid, job_id=lease["job_id"])
            return

        # everything else accepts first
        await self.client.post(f"/leases/{lid}/respond", json={"accept": True},
                                headers=self._headers())
        self._record("accepted_lease", lease_id=lid, job_id=lease["job_id"])

        asyncio.create_task(self._run_job(lease))

    async def _run_job(self, lease: dict):
        lid = lease["id"]
        job = (await self.client.get(f"/jobs/{lease['job_id']}")).json()
        duration = job["duration_sec"]
        behavior = self.cfg.behavior

        if behavior == "normal":
            await asyncio.sleep(duration)
            await self.client.post(f"/leases/{lid}/complete",
                                    json={"success": True, "artifact_uploaded": True},
                                    headers=self._headers())
            self._record("completed_job", lease_id=lid, outcome="success")

        elif behavior == "wrong_capabilities":
            # claimed enough VRAM at registration to win the match, but the
            # "hardware" can't actually do the work -> fails partway through
            await asyncio.sleep(min(duration, 1.5))
            await self.client.post(f"/leases/{lid}/complete",
                                    json={"success": False, "artifact_uploaded": False,
                                          "reason": "capability_mismatch"},
                                    headers=self._headers())
            self._record("completed_job", lease_id=lid, outcome="capability_mismatch")

        elif behavior == "upload_fail":
            await asyncio.sleep(duration)
            await self.client.post(f"/leases/{lid}/complete",
                                    json={"success": True, "artifact_uploaded": False,
                                          "reason": "artifact_upload_failed"},
                                    headers=self._headers())
            self._record("completed_job", lease_id=lid, outcome="upload_fail")

        elif behavior == "disk_full":
            await asyncio.sleep(min(duration, 1.0))
            await self.client.post(f"/leases/{lid}/complete",
                                    json={"success": False, "artifact_uploaded": False,
                                          "reason": "disk_full"},
                                    headers=self._headers())
            self._record("completed_job", lease_id=lid, outcome="disk_full")

        elif behavior == "drain_mid_job":
            meets_grace = self.cfg.params.get("meets_grace", False)
            await asyncio.sleep(duration * 0.4)
            await self.client.post(f"/providers/{self.id}/drain", headers=self._headers())
            self._record("drained_mid_job", lease_id=lid, meets_grace=meets_grace)
            remaining = duration * 0.6
            if not meets_grace:
                remaining += self.cfg.params.get("overshoot_sec", 30)
            await asyncio.sleep(remaining)
            try:
                await self.client.post(f"/leases/{lid}/complete",
                                        json={"success": True, "artifact_uploaded": True},
                                        headers=self._headers())
                self._record("completed_job", lease_id=lid, outcome="finished_after_drain")
            except httpx.HTTPError:
                # coordinator likely already reclaimed the lease after grace expired
                self._record("completed_job", lease_id=lid, outcome="reclaimed_before_finish")

        elif behavior == "disappear":
            # If it managed to accept a lease before going dark, it goes
            # dark *on* that lease too: no completion call, ever. The
            # coordinator's heartbeat monitor is what has to notice and
            # reclaim this -- not any signal from the job itself.
            self._record("went_dark_mid_job", lease_id=lid)
            return

        elif behavior in ("clock_skew",):
            # normal execution; the interesting behavior is in heartbeat_loop
            await asyncio.sleep(duration)
            await self.client.post(f"/leases/{lid}/complete",
                                    json={"success": True, "artifact_uploaded": True},
                                    headers=self._headers())
            self._record("completed_job", lease_id=lid, outcome="success")

        else:
            await asyncio.sleep(duration)
            await self.client.post(f"/leases/{lid}/complete",
                                    json={"success": True, "artifact_uploaded": True},
                                    headers=self._headers())
            self._record("completed_job", lease_id=lid, outcome="success")

    async def run(self):
        await self.register()
        await asyncio.gather(self.heartbeat_loop(), self.lease_loop())

    def stop(self):
        self._stopped = True


async def submit_jobs(client: httpx.AsyncClient, job_specs: List[dict], log: List[dict]):
    submitted = []
    for spec in job_specs:
        count = spec.get("count", 1)
        body = {k: v for k, v in spec.items() if k != "count"}
        for _ in range(count):
            resp = await client.post("/jobs", json=body)
            resp.raise_for_status()
            j = resp.json()
            submitted.append(j["id"])
            log.append({"t": round(time.time(), 3), "event": "job_submitted",
                         "job_id": j["id"], "spec": body})
    return submitted


async def run_scenario(cfg: dict) -> dict:
    """Runs one scenario end-to-end and returns a report dict. Callable
    directly from tests, not just from the CLI entrypoint."""
    random.seed(cfg.get("seed", 0))
    base_url = cfg["coordinator_url"]
    heartbeat_interval = cfg.get("heartbeat_interval_sec", 3)
    poll_interval = cfg.get("poll_interval_sec", 1)
    run_duration = cfg.get("run_duration_sec", 60)

    log: List[dict] = []

    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
        job_ids = await submit_jobs(client, cfg.get("jobs", []), log)

        providers = [
            FakeProvider(
                client, ProviderConfig(
                    name=p["name"], behavior=p["behavior"], capabilities=p["capabilities"],
                    tiers=p.get("tiers", ["standard"]), reclaim_policy=p.get("reclaim_policy", "graceful"),
                    params={k: v for k, v in p.items()
                            if k not in ("name", "behavior", "capabilities", "tiers", "reclaim_policy")},
                ),
                heartbeat_interval, poll_interval, log,
            )
            for p in cfg.get("providers", [])
        ]

        tasks = [asyncio.create_task(p.run()) for p in providers]
        await asyncio.sleep(run_duration)
        for p in providers:
            p.stop()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        final_status = (await client.get("/status")).json()
        final_jobs = (await client.get("/jobs")).json()
        final_leases = (await client.get("/leases")).json()
        final_providers = (await client.get("/providers")).json()

    return {
        "log": log,
        "final_status": final_status,
        "jobs": final_jobs,
        "leases": final_leases,
        "providers": final_providers,
        "submitted_job_ids": job_ids,
    }
