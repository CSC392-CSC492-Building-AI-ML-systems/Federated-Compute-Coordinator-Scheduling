# Coordinator architecture skeleton

**This is a team design scaffold with partially implemented behavior.** Several
business operations still raise `NotImplementedError`. Models and service
contracts continue to evolve as the team implements features.

| Location | Responsibility |
| --- | --- |
| `app.py` | Assemble FastAPI and routers |
| `api/` | Handle HTTP input/output and call services |
| `services/` | Implement complete business operations |
| `policies/` | Decide matching, retry, and health rules |
| `models/` | Define data fields and states |
| `store.py` | Own shared data and locking |
| `scheduler.py` | Match waiting jobs to providers |
| `monitor.py` | Handle timeouts and background checks |

Normal flow: **API → Service → Store**. Services and background workers use
policies to make decisions. An update involving a Job, Lease, and Provider
should share one atomic boundary; the team will implement that boundary.

Start reading with `api/providers.py` and `services/provider_service.py`.
Leave remaining business rules, authentication, and lifecycle details for team
implementation. Provider registration and the monitor are wired. The heartbeat
API scaffold is wired, with its service implementation still pending.

`POST /providers/{provider_id}/heartbeat` accepts an optional JSON body containing
`provider_time` for logging. The route passes the Coordinator's receive time to
the service and maps its not-found/drained exceptions to 404/409. Implement
`services/provider_service.py::heartbeat` before using this endpoint with real
Providers; API tests currently replace that service with a fake.

```powershell
python -m pip install -r requirements.txt
python -m pytest tests/ -v
uvicorn coordinator.app:app --reload
```

The server exposes wired routes and FastAPI's documentation. CI runs
skeleton import/layout checks and pre-commit style checks when a PR opens, reopens, or receives commits.
A green check means the scaffold is intact, **not that backend features work**.
Add behavior tests as the team implements each feature.

Before committing, install the local Git hook once:

```powershell
python -m pre_commit install
```

After that, `git commit` automatically runs basic file checks plus Ruff's
style, import-order, and formatting checks. Use `python -m pre_commit run
--all-files` to run the same checks manually.

`docs/`, `ANALYSIS.md`, and `harness/` are earlier design/reference material;
they do not describe implemented functionality in this scaffold. The old
implementation and its tests were archived outside the project before removal.
