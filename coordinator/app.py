"""Wire the application together."""

import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from typing import Callable, Optional

from fastapi import FastAPI

from coordinator.api import jobs, leases, providers, status
from coordinator.api.tools.errors import install_error_handlers
from coordinator.monitor import monitor_loop
from coordinator.scheduler import scheduler_loop
from coordinator.settings import Settings, load_settings
from coordinator.store import Store


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def create_app(
    clock: Callable[[], datetime] = utc_now, settings: Optional[Settings] = None
) -> FastAPI:
    """Build an app with its own Store. Settings come from config/coordinator.yaml
    unless a test passes its own."""
    settings = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Created here, inside the running event loop, so the Store's asyncio.Lock
        # belongs to the loop that serves requests.
        app.state.store = Store()
        app.state.clock = clock
        app.state.settings = settings
        app.state.monitor_task = None
        app.state.scheduler_task = None

        # IMPORTANT: monitor =)
        app.state.monitor_task = asyncio.create_task(monitor_loop(app.state.store, clock, settings))
        app.state.scheduler_task = asyncio.create_task(
            scheduler_loop(app.state.store, clock, settings)
        )

        yield

        # Stop the monitor on shutdown so it does not outlive the app.
        if app.state.monitor_task is not None:
            app.state.monitor_task.cancel()
            with suppress(asyncio.CancelledError):
                await app.state.monitor_task

        # Stop the scheduler on shutdown so it does not outlive the app.
        if app.state.scheduler_task is not None:
            app.state.scheduler_task.cancel()
            with suppress(asyncio.CancelledError):
                await app.state.scheduler_task

    app = FastAPI(title="Project 16 =)", lifespan=lifespan)
    for router in (providers.router, jobs.router, leases.router, status.router):
        app.include_router(router)
    install_error_handlers(app)
    return app


app = create_app()
