"""Shared FastAPI dependencies: hand the Store, the settings and the current time to routes.

Why this file exists (it is not in the original scaffold layout):
- Routes need to pass "the one Store" to services, but must not create it themselves.
  If each route did `Store()`, every request would get its own empty Store.
- The app creates one Store at startup and keeps it on app.state; these functions
  just fetch it. Routes still never read or write Store data: they only pass the
  object to a service. So the flow stays API -> Service -> Store.
- Tests build a new app per test (create_app), so each test gets a fresh Store and
  a fixed clock without touching global state.
"""

from datetime import datetime

from fastapi import Request

from coordinator.settings import Settings
from coordinator.store import Store


def get_store(request: Request) -> Store:
    # The app owns one Store (created in its lifespan). Reading it from app.state
    # lets each test app get a fresh, isolated Store.
    return request.app.state.store


def get_settings(request: Request) -> Settings:
    # create_app loads the settings once. A test app can pass its own.
    # Routes hand services only the numbers they need, not the whole Settings.
    return request.app.state.settings


def get_received_at(request: Request) -> datetime:
    # Resolved before the handler runs, so the time is taken when the request
    # arrives, not after waiting for the Store lock.
    return request.app.state.clock()
