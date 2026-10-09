"""Run a scenario file against a coordinator, check the results, and write a report.

Usage: python -m harness.runner harness/scenarios/<scenario>.yaml

Run manually during development, not in CI. A failed check is logged and recorded
in the report; it never stops the run.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import yaml

from harness.client import CoordinatorClient
from harness.provider import FakeProvider


async def run_scenario(config_path):
    """Submit jobs, start providers, wait, and return the event log and final state."""
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

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
            behavior=prov["behavior"],
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
    for job_config in config["jobs"]:
        for _ in range(job_config["count"]):
            # change with client.py
            job_payload = {
                "required_vram_mb": job_config["min_vram_mb"],
                # Default, likely change
                "required_runtime": "cuda12",
                "tier": job_config["tier"],
            }
            await client.submit_job(job_payload)

    # main loop, check this for range update
    for sec in range(1, max_duration + 1):
        v_time_now += timedelta(seconds=1)
        print(f"--- Time: {sec} ---")
        for prov in providers:
            # time skip
            await prov.tick(v_time_now)
            # Event Logs
            while prov.events:
                event = prov.events.pop(0)
                print(f"[{event['provider']}] {event['event']}")

    # clean client
    await client.close()


def main():
    asyncio.run(run_scenario("harness/scenarios/default.yaml"))


if __name__ == "__main__":
    main()
