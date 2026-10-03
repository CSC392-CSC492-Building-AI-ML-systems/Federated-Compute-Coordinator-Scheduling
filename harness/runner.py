"""Run a scenario file against a coordinator, check the results, and write a report.

Usage: python -m harness.runner harness/scenarios/<scenario>.yaml

Run manually during development, not in CI. A failed check is logged and recorded
in the report; it never stops the run.
"""
import yaml
import asyncio
from provider import FakeProvider
from datetime import datetime, timedelta, timezone

# REPLACEMENT CLIENT
class FakeClient:
    async def register_provider(self, payload):
        print(f"  [API MOCK] Registered {payload['provider_id']} with {payload['capabilities']['vram_mb']} MB VRAM")
        return {"status": "ACTIVE"}

    async def heartbeat(self, provider_id):
        # We will keep this silent so it doesn't spam the console
        pass 

    async def get_lease_offers(self, provider_id):
        # Returning empty list for now so providers just idle
        return [] 
        
    async def accept(self, lease_id, provider_id):
        pass
        
    async def started(self, lease_id, provider_id):
        pass
        
    async def report(self, lease_id, provider_id, outcome):
        pass

async def run_scenario(config_path):
    """Submit jobs, start providers, wait, and return the event log and final state."""
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    # PLACEHOLDER
    client = FakeClient()
    providers = []

    # PROVIDER CONFIG
    for prov in config['provider']:
        provider = FakeProvider(
            client = client,
            provider_id = prov['capabilities']['gpu_model'],
            # TODO: change to mb likely
            vram_mb = prov['capabilities']['vram_mb'],
            runtimes = prov['capabilities']['runtimes'],
            accepted_tiers=prov['tiers'],
            behavior=prov['behavior']
            )
    providers.append(provider)

    # JOB CONFIG
    job_durations = {}
    job_payloads = []
    job_counter = 1

    for job_config in config['jobs']:
        for _ in range(job_config['count']):
            job_id = f"job-{job_counter}"
            job_durations[job_id] = job_config['duration_sec']
            # Build the payload strictly matching the API contract
            job_payloads.append({
                "required_vram_mb": job_config['min_vram_mb'],
                # Default, likely change
                "required_runtime": "cuda12",
                "tier": job_config['tier']
            })
            job_counter += 1

    # main loop
    v_time = 0
    max_duration = config.get('run_duration_sec', 45)
    while v_time < max_duration:
        for provider in providers:

            pass

    
    


def main():
    """Load the scenario YAML, run it, run its checks, log PASS/FAIL, write the JSON report."""
    raise NotImplementedError("Team implementation pending")

if __name__ == "__main__":
    main()
