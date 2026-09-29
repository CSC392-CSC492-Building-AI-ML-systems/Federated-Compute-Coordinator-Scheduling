"""Wire the application together."""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Callable

from fastapi import FastAPI

from coordinator.api import jobs, leases, providers, status
from coordinator.api.tools.errors import install_error_handlers
from coordinator.store import Store


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def create_app(clock: Callable[[], datetime] = utc_now) -> FastAPI:
    """Build an app with its own Store. Tests pass a fixed clock for exact timestamps."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Created here, inside the running event loop, so the Store's asyncio.Lock
        # belongs to the loop that serves requests.
        app.state.store = Store()
        app.state.clock = clock
        yield
        # TODO: Start and stop the monitor when the team implements background work.

    app = FastAPI(title="Project 16 =)", lifespan=lifespan)
    for router in (providers.router, jobs.router, leases.router, status.router):
        app.include_router(router)
    install_error_handlers(app)
    return app


app = create_app()
