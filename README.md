# Coordinator architecture skeleton

**This is a team design scaffold, not a working backend.** Business operations
raise `NotImplementedError`; model classes are placeholders. Function names
and signatures are examples to discuss, not a finalized API contract.

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
Leave business rules, data fields, authentication, and lifecycle details for
team implementation. No business endpoints or background loops are wired yet.

```powershell
python -m pip install -r requirements.txt
python -m pytest tests/ -v
uvicorn coordinator.app:app --reload
```

The server currently provides only FastAPI's documentation routes. CI runs
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

`harness/` is the fake-provider harness scaffold; see `harness/README.md` for
its layout and team split. `docs/` and `ANALYSIS.md` are earlier
design/reference material and do not describe implemented functionality. The
old implementation and its tests were archived outside the project before removal.
