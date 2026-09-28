"""Run a scenario file against a coordinator, check the results, and write a report.

Usage: python -m harness.runner harness/scenarios/<scenario>.yaml

Run manually during development, not in CI. A failed check is logged and recorded
in the report; it never stops the run.
"""


async def run_scenario(config):
    """Submit jobs, start providers, wait, and return the event log and final state."""
    raise NotImplementedError("Team implementation pending")


def main():
    """Load the scenario YAML, run it, run its checks, log PASS/FAIL, write the JSON report."""
    raise NotImplementedError("Team implementation pending")


if __name__ == "__main__":
    main()
