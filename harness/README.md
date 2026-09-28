# Fake-provider harness skeleton

**This is a team design scaffold, not a working harness.** Operations raise
`NotImplementedError`. Names and signatures are examples to discuss.
`CSC398_API_CONTRACT` is the reference for all coordinator calls.

| Location | Responsibility |
| --- | --- |
| `client.py` | Call coordinator endpoints over HTTP |
| `provider.py` | Simulate one provider; branch on `behavior` at each lifecycle step |
| `runner.py` | Load a scenario, run it, run its checks, write a JSON report |
| `behaviors/` | What a faulty provider does, grouped by lifecycle step |
| `checks/` | What the coordinator should do in response; mirrors `behaviors/` |
| `checks/invariants.py` | Contract invariants checked on every run |
| `scenarios/` | Scenario YAML files |

Flow: **runner → FakeProvider (+ behaviors) → CoordinatorClient → coordinator**,
then **runner → checks → report**.

## Running

```powershell
python -m harness.runner harness/scenarios/default.yaml
```

Scenarios run manually during development, not in CI. A failed check means the
harness found something; it is logged and recorded in the report, and the run
continues. Known coordinator bugs are tracked the same way, through logging.
CI only checks that harness modules exist and import.

## Team split

1. Together: build the client, the `normal` behavior, and the runner until the
   default scenario runs.
2. Then each person owns one lifecycle group, both the behavior and its check:
   - A: `liveness` (disappear, clock_skew)
   - B: `lease` (reject_lease, capability_lie)
   - C: `execution` (drain_mid_job, upload_fail, disk_full) and `invariants`

## Open design decisions

- Scenario file format and per-behavior parameters.
- Real time vs. virtual time for repeatable runs.
- How the harness reaches a coordinator (running server or in-process).
- Statuses and reasons come from `coordinator.models` once those enums exist.
