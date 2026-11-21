"""Run a scenario file against a coordinator, check the results, and write a report.

Usage: python -m harness.runner harness/scenarios/<scenario>.yaml

Run manually during development, not in CI. A failed check is logged and recorded
in the report; it never stops the run.
"""

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import yaml

from harness.checks import run_checks
from harness.client import CoordinatorClient
from harness.errors import CoordinatorError
from harness.provider import FakeProvider


async def run_scenario(config_path):
    """Submit jobs, start providers, wait, then check the coordinator's final state
    and return the check results."""
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    # Params priority: shared.yaml < scenario-level values < provider's own entry
    with open(Path(config_path).parent / "shared.yaml") as file:
        shared_params = yaml.safe_load(file)
    shared_params.update({k: v for k, v in config.items() if k not in ("providers", "jobs")})

    client = CoordinatorClient(base_url="http://localhost:8000", api_key="test-key")
    providers = []

    # PROVIDER CONFIG
    for prov in config["providers"]:
        provider = FakeProvider(
            client=client,
            provider_id=prov["name"],
            gpu_model=prov["capabilities"]["gpu_model"],
            vram_mb=prov["capabilities"]["vram_mb"],
            runtimes=prov["capabilities"]["runtimes"],
            accepted_tiers=prov["tiers"],
            # driver_version = prov['capabilities']['driver'],
            behavior=prov.get("behavior", "normal"),
            params={**shared_params, **prov},
        )
        providers.append(provider)

    # to discuss when time and tick approach changes
    v_time_now = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    max_duration = config.get("run_duration_sec", 45)
    print("--- Running Scenerio ---")

    # regester provider at time 00
    for prov in providers:
        await prov.register(v_time_now)

    # submit job after provider
    job_ids = []
    for job_config in config["jobs"]:
        for _ in range(job_config["count"]):
            job_payload = {
                "tier": job_config["tier"],
                "required_vram_mb": job_config["min_vram_mb"],
                "required_runtime": job_config.get("required_runtime", "cuda12"),
                "gpu_models": job_config.get("gpu_models", []),
            }
            job = await client.submit_job(job_payload)
            job_ids.append(job["job_id"])

    # main loop, check this for range update
    # Every provider's events, kept for the checks
    events = []
    for sec in range(1, max_duration + 1):
        v_time_now += timedelta(seconds=1)
        print(f"--- Time: {sec} ---")
        for prov in providers:
            # time skip
            await prov.tick(v_time_now)
            # Event Logs
            while prov.events:
                event = prov.events.pop(0)
                events.append(event)
                print(f"[{event['provider']}] {event['event']}")
        await asyncio.sleep(1)

    try:
        final_state = await collect_final_state(client, events, job_ids)
    except (CoordinatorError, httpx.TransportError) as error:
        final_state = {"error": f"{type(error).__name__}: {error}"}
    finally:
        await client.close()

    results = run_checks(events, final_state)
    print_results(results)
    write_report(config_path, events, final_state, results)
    return results


async def collect_final_state(client, events, job_ids):
    """Ask the coordinator for its records at the end of the run, for the checks.
    Providers are read from the "registered" events, and the leases are read from the providers'
    events, and each job's current lease.
    """
    provider_ids = sorted({e["provider"] for e in events if e["event"] == "registered"})
    providers = {pid: await client.get_provider(pid) for pid in provider_ids}
    jobs = {job_id: await client.get_job(job_id) for job_id in job_ids}
    lease_ids = {e["lease_id"] for e in events if e.get("lease_id")}
    lease_ids |= {job["current_lease_id"] for job in jobs.values() if job.get("current_lease_id")}
    leases = {lease_id: await client.get_lease(lease_id) for lease_id in sorted(lease_ids)}
    return {
        "status": await client.get_status(),
        "providers": providers,
        "jobs": jobs,
        "leases": leases,
    }


def print_results(results):
    """Prints one line per check result, then a summary."""
    print("--- Checks ---")
    for r in results:
        detail = f": {r['detail']}" if r["detail"] else ""
        print(f"{r['status']}  {r['check']}{detail}")
    summary = [
        f"{sum(r['status'] == status for r in results)} {status}"
        for status in ("PASS", "FAIL", "NOT_IMPLEMENTED")
    ]
    print(", ".join(summary))


def write_report(config_path, events, final_state, results):
    """Save the run as <scenario>_report.json next to the scenario file
    (git ignores these files). Returns the report's path."""
    path = Path(config_path).with_name(Path(config_path).stem + "_report.json")
    report = {
        "scenario": Path(config_path).stem,
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            status: sum(r["status"] == status for r in results)
            for status in ("PASS", "FAIL", "NOT_IMPLEMENTED")
        },
        "results": results,
        "final_state": final_state,
        "events": events,
    }
    # default=str: write anything JSON cannot store (like a datetime) as text
    path.write_text(json.dumps(report, indent=2, default=str))
    print(f"Report written to {path}")
    return path


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else "harness/scenarios/default.yaml"
    asyncio.run(run_scenario(config_path))


if __name__ == "__main__":
    main()
