"""Wire the application together. Business endpoints are not implemented yet."""

from fastapi import FastAPI

from coordinator.api import jobs, leases, providers, status

app = FastAPI(title="Project 16 =)")
for router in (providers.router, jobs.router, leases.router, status.router):
    app.include_router(router)

# TODO: Start and stop the monitor when the team implements background work.
