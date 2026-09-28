#!/usr/bin/env python3
"""
CLI entrypoint: run a scenario file against a running coordinator instance.

    python harness/run_harness.py harness/scenarios/default.yaml

Writes a JSON report (event log + final state snapshot) next to the scenario
file, named <scenario>_report.json, so results can be diffed across runs.
"""
import asyncio
import json
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from harness.harness import run_scenario  # noqa: E402


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <scenario.yaml>")
        sys.exit(1)

    scenario_path = Path(sys.argv[1])
    cfg = yaml.safe_load(scenario_path.read_text())

    print(f"Running scenario: {scenario_path.name}")
    print(f"  coordinator: {cfg['coordinator_url']}")
    print(f"  providers:   {len(cfg.get('providers', []))}")
    print(f"  duration:    {cfg.get('run_duration_sec', 60)}s")
    print()

    report = asyncio.run(run_scenario(cfg))

    out_path = scenario_path.with_name(scenario_path.stem + "_report.json")
    out_path.write_text(json.dumps(report, indent=2, default=str))

    print("=== Final status ===")
    print(json.dumps(report["final_status"], indent=2))
    print()
    print(f"Full report written to {out_path}")


if __name__ == "__main__":
    main()
